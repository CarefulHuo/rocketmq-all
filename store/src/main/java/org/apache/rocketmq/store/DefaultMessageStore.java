/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.channel.Channel;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * RocketMQ 存储核心类，Broker 持有。包含了很多对存储文件进行操作的 API，其他模块对消息实体的操作都是通过该类进行的。
 * <p>
 * 说明：
 * 1. RocketMQ 存储使用的本地文件存储系统，效率高也可靠。
 * 2. 主要涉及到三种类型的文件：CommitLog、ConsumeQueue、IndexFile。
 * - CommitLog：所有主题的消息都存在 CommitLog 中，单个 CommitLog 默认大小是 1GB，并没文件名以起始偏移量为文件名。固定 20位，不足前面补 0.
 *   比如 00000000000000000000 代表第一个文件，第二个文件名就是 00000000001073741824 表明起始偏移量是 1073741824。
 *   以这样的方式命名用偏移量就能找到对应的文件。所有消息都是顺序写入的，超过文件大小就开启下一个文件。
 * - ConsumeQueue：
 * 1）消息消费队列，可以认为是 CommitLog 中的消息索引，因为 CommitLog 中的消息包含了所有主题的，所以通过索引才能更高效的查找消息
 * 2）ConsumeQueue 存储的条目是固定大小，只会存储 8 字节的 CommitLog 物理偏移量 + 4 字节的消息长度 + 8 字节的消息 Tag 的哈希值，固定 20字节。
 * 3）在实际存储中，ConsumeQueue 对应的是某个 Topic 下的某个 Queue，每个文件约 5.72M，由 30w 条数据组成，即 30w * 20byte
 * 4) 消费者是先从 ConsumeQueue 来得到消息真实的物理地址，然后再去 CommitLog 获取消息
 * - IndexFile：
 * 1）索引文件，是额外提供查询消息的手段，不影响主流程
 * 2）通过消息id、key或者时间区间来查询对应的消息，文件名以创建时间戳命名，固定的单个 IndexFile大小约为 400MB，一个 IndexFile 存储 2000w 个索引。
 * <p>
 * 消息存储流程：
 * 1）消息到了，先存储到 CommitLog ，然后会有一个 ReputMessageService 线程近乎实时地将消息转发给消息消费队列文件与索引文件，也就是说异步生成的。
 * 2）CommitLog 采用混合存储，也就是所有的 Topic 都存储在一起，顺序追加写入，文件名是用起始偏移量命名
 * 3）消息先写入 CommitLog，再通过后台线程分发到 ConsumeQueue 和 IndexFile 中。
 * 4）消费者先读取 ConsumeQueue 得到消息真正的物理偏移量，然后访问 CommitLog 得到真正的消息。
 * 5）利用了 mmap 机制少了一次拷贝，利用文件预分配和文件预热提升性能
 * 6）提供同步和异步刷盘，根据场景选择合适的刷盘机制
 * <p>
 * 文件清除机制 - {@link DefaultMessageStore#addScheduleTask()} 使用的是定时任务的方式，在 Broker 启动时会触发 {@link org.apache.rocketmq.store.DefaultMessageStore#start()}
 * 1) RocketMQ 操作 CommitLog 和 ConsumeQueue 文件，都是基于内存映射文件(MappedFile)，Broker 在启动时加载 CommitLog、ConsumeQueue 对应的文件夹，读取物理文件到内存以创建对应的内存映射文件。
 *    为了避免内存与磁盘的浪费，不可能将消息永久存储在消息服务器上，所以需要一种机制来删除已过期的文件。
 * 2）RocketMQ 顺序写 CommitLog 、ConsumeQueue 文件，所有写操作全部落在最后一个 CommitLog 或 ConsumeQueue 对应的 MappedFile 文件上，之前的文件在下一个文件创建后，将不会再被更新
 * 3）RocketMQ 清除过期文件的方法是：如果非当前写文件在一定时间间隔内没有再次被更新，则认为是过期文件，可以被删除，RocketMQ 不会管这个文件上的消息是否被全部消费，默认每个文件的过期时间是 72 小时。
 * 4）消息是被顺序存储在 CommitLog 文件的，并且消息大小不定长，所以消息的清理是不可能以消息为单位进行清理的，而是以 CommitLog 文件为单位进行清理的，否则会急剧下降清理效率，并且删除逻辑的实现也会很复杂。
 * 额外说明：
 * 1）用户应该在磁盘使用率达到 0.85 之前监控磁盘使用率。RocketMQ 设置了两个级别的服务退化：
 *   - 当磁盘使用率达到 0.85 时，立即删除最旧的文件，以缓解可用服务(RW(读和写))的磁盘压力；
 *   - 当磁盘使用率达到 0.9 时，禁止对可用服务(R)进行写操作。
 *
 */
public class DefaultMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 消息存储配置属性
     * 存储相关的属性配置：存储路径、CommitLog/ConsumeQueue 文件大小、刷盘频次、清理文件时间间隔等
     */
    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    /**
     * CommitLog 存储文件的具体实现类
     * <p>
     * 一个即可，因为所有主题的消息都存储在 CommitLog 中
     */
    private final CommitLog commitLog;

    /**
     * topic 的队列文件缓存表，按消息主题分组
     * <p>
     * 针对某个 Topic，对于每个队列，都有对应的 ConsumeQueue 文件，存储了消息的偏移量、消息长度、消息 Tag 的哈希值
     * @see DefaultMessageStore#findConsumeQueue(String topic, int queueId)
     * @see DefaultMessageStore#loadConsumeQueue()
     */
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    /**
     * ConsumeQueue 刷盘任务线程
     */
    private final FlushConsumeQueueService flushConsumeQueueService;

    /**
     * 清理 CommitLog 过期文件的任务线程
     */
    private final CleanCommitLogService cleanCommitLogService;

    /**
     * 清理 ConsumeQueue 过期文件的任务线程
     */
    private final CleanConsumeQueueService cleanConsumeQueueService;

    /**
     * Index 文件实现类
     */
    private final IndexService indexService;

    /**
     * 预分配 MappedFile 文件线程，RocketMQ 使用内存映射处理 CommitLog 、ConsumeQueue 文件
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * CommitLog 消息分发线程任务，根据 CommitLog 文件构建 ConsumeQueue 、Index 文件
     * todo 重要，理解 CommitLog 和 ConsumeQueue 文件的关系
     */
    private final ReputMessageService reputMessageService;

    /**
     * 存储高可用机制
     * 主从同步的实现类
     */
    private final HAService haService;

    /**
     * 延时消息任务线程，执行延时消息任务
     */
    private final ScheduleMessageService scheduleMessageService;

    /**
     * 存储统计任务线程
     */
    private final StoreStatsService storeStatsService;

    /**
     * 消息堆外内存池 ByteBuffer 池
     */
    private final TransientStorePool transientStorePool;

    /**
     * 存储服务状态
     */
    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));

    /**
     * Broker 统计服务
     */
    private final BrokerStatsManager brokerStatsManager;

    /**
     * todo 在消息拉取长轮询模式下的消息达到监听器
     */
    private final MessageArrivingListener messageArrivingListener;

    /**
     * Broker 配置属性
     */
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    /**
     * 文件刷盘监测点
     */
    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    /**
     * todo CommitLog 文件转发请求
     * <p>
     * 转发 CommitLog 日志，主要是从 CommitLog 日志转发到 ConsumeQueue 、Index 文件
     * ConsumeQueue ---> CommitLogDispatcherBuildConsumeQueue
     * index ---> CommitLogDispatcherBuildIndex
     */
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    private final ScheduledExecutorService diskCheckScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DiskCheckScheduledThread"));

    /**
     * 消息存储核心类，集中-存储相关的核心类
     * @param messageStoreConfig
     * @param brokerStatsManager
     * @param messageArrivingListener
     * @param brokerConfig
     * @throws IOException
     */
    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
        final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        // 消息到达的监听器
        this.messageArrivingListener = messageArrivingListener;
        // Broker 的配置信息
        this.brokerConfig = brokerConfig;
        // 消息存储配置信息
        this.messageStoreConfig = messageStoreConfig;
        // Broker的统计管理器
        this.brokerStatsManager = brokerStatsManager;
        // 创建预分配 CommitLog 文件映射的服务
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        // 初始化一个 CommitLog
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }
        // 消息消费队列
        this.consumeQueueTable = new ConcurrentHashMap<>(32);
        // consumerQueue 刷盘服务，仅仅只有异步刷盘
        this.flushConsumeQueueService = new FlushConsumeQueueService();
        // 删除 CommitLog 过期文件的任务
        this.cleanCommitLogService = new CleanCommitLogService();
        // 删除 ConsumeQueue 过期文件 && index 无效文件的任务
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        // 创建统计服务
        this.storeStatsService = new StoreStatsService();
        // 创建索引服务 index 文件(索引文件)
        this.indexService = new IndexService(this);

        // 如果开启 dledger 则不需要 HAService
        // 创建 主从复制的 HaService 对象
        // todo 主从同步不具备主从切换功能，也就是当主节点宕机之后，从节点不会接管消息发送，但可以提供消息读取
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService = new HAService(this);
        } else {
            this.haService = null;
        }

        // 创建消息重放服务
        this.reputMessageService = new ReputMessageService();
        // 延时队列服务
        this.scheduleMessageService = new ScheduleMessageService(this);
        // 堆外内存池
        this.transientStorePool = new TransientStorePool(messageStoreConfig);
        // todo 是否使用堆外内存，其中一项必须是异步刷盘，因为同步刷盘是直接把内存中的数据，写到文件里面了
        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }
        // 启动预分配 CommitLog 文件映射的服务
        this.allocateMappedFileService.start();
        // 启动 index 索引文件服务
        this.indexService.start();

        // 创建 CommitLog 派发器，目前有两个实现，一个是构建 ConsumeQueue，一个是构建 Index
        // todo 改造延时消息的话，可以新定义一个 Dispatcher , 专门派发延时消息类型的派发请求
        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        lockFile = new RandomAccessFile(file, "rw");
    }

    /**
     * 根据 CommitLog 文件中有效的物理偏移量，清除无效的 ConsumeQueue 文件
     * @param phyOffset CommitLog 文件中最大的物理偏移量
     */
    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * 在 RocketMQ 启动过程中，是如何根据 CommitLog 重构 ConsumeQueue 、index 的。因为比较 CommitLog 文件中的消息与 ConsumeQueue 中的文件内容，并不能确保是一致的
     * <p>
     * 加载过程如下：
     * 1. 加载相关文件到内存(内存映射文件 MappedFile)
     *  - CommitLog 文件
     *  - ConsumeQueue 文件
     *  - 存储检测点(CheckPoint)文件
     *  - 索引 indexFile 文件
     * 2. 执行文件恢复，恢复顺序如下：
     *  - 先恢复 ConsumeQueue 文件，把不符合的 ConsumeQueue 文件删除，一个 ConsumeQueue 存储的条目正确的标准是(存储的 CommitLog 物理偏移量 > 0 && 消息大小 > 0)，从倒数第三个文件开始恢复
     *  - 如果 abort 文件存在，此时找到第一个正常的 CommitLog 文件，然后对该文件重新进行转发，依次更新 ConsumeQueue、index 文件(非正常逻辑恢复)；正常逻辑恢复：与恢复 ConsumeQueue 文件类似
     *
     * @throws IOException
     * @see org.apache.rocketmq.broker.BrokerController#initialize()
     */
    public boolean load() {
        boolean result = true;

        try {
            // 1. 判断 /user.home/store/abort 文件是否存在，
            // abort 文件在 Broker 在启动过程中创建，在退出时，通过注册 JVM 钩子函数 删除 abort 文件，也就是说如果存在，则说明上一次 Broker 退出的时候，是不正常的
            // todo 在 Broker 异常退出时，CommitLog 与 ConsumerQueue 数据有可能不一致，需要进行恢复
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            // 2. 需要处理延迟消息，包括维护 delayLevelTable 映射表和加载文件
            if (null != scheduleMessageService) {
                result = result && this.scheduleMessageService.load();
            }

            // load Commit Log
            // 3. 加载 CommitLog  文件
            result = result && this.commitLog.load();

            // load Consume Queue
            // 4. 加载 ConsumeQueue 文件
            result = result && this.loadConsumeQueue();

            // 以上都正常加载后，加载并存储 checkpoint 文件
            if (result) {
                // 5. 文件存储检测点封装对象
                this.storeCheckpoint =
                    new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                // 6. 索引文件加载
                this.indexService.load(lastExitOK);

                // 7.根据是否异常退出，恢复文件
                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());
            }
        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * todo 非常重要
     *  存储服务会启动多个后台线程
     *
     * @throws Exception
     * @see org.apache.rocketmq.broker.BrokerController#start()
     */
    public void start() throws Exception {

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);

        /**************************todo 获取CommitLog 最小物理偏移量，用于指定从哪个物理偏移量开始转发消息给 ConsumeQueue 和 index 文件 ***********************************/
        {
            /**
             * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
             * 1. 确保在恢复过程中根据提交日志的最大物理偏移量截断快进消息;
             * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
             * 2. DLedger committedPos 可能丢失，因此 maxPhysicalPosInLogicQueue 可能比 DLedgerCommitLog 返回的 maxOffset 大，让它去吧;
             * 3. Calculate the reput offset according to the consume queue;
             * 3. 根据消费队列计算重新输入偏移量;
             * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
             * 4. 在启动提交日志之前，请确保要调度的滞后消息，尤其是在自动更改代理角色时。
             */
            // 遍历队列，获取最大偏移量，因为重放消息，不能重复。先看 ConsumeQueue 已经重放消息到哪了。如果写入位置大于 CommitLog 最小物理偏移量，那么就以写入的位置开始，重放消息。
            long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
            for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
                // 消息索引是是分配到不同的队列的，所以需要找到其中最大的消息索引
                for (ConsumeQueue logic : maps.values()) {
                    if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
                        maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
                    }
                }
            }
            if (maxPhysicalPosInLogicQueue < 0) {
                maxPhysicalPosInLogicQueue = 0;
            }
            if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
                maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
                /**
                 * This happens in following conditions:
                 * 1. If someone removes all the consumequeue files or the disk get damaged.
                 * 1. 如果有人删除了所有 ConsumeQueue 文件或磁盘损坏
                 * 2. Launch a new broker, and copy the commitlog from other brokers.
                 * 2. 启动一个新的代理，并从其他代理复制提交日志。
                 *
                 * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
                 * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
                 * 所有条件都具有相同的共同点，即 maxPhysicalPosInLogicQueue 应为 0。如果 maxPhysicalPosInLogicQueue 为 gt 0，则可能有问题。
                 */
                log.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
            }
            log.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
                maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());


            /*************** todo 重放消息开始位置 & 启动重放消息的 CommitLog 的任务构建 ConsumeQueue 和 IndexFile & 启动 CommitLog 和 ConsumeQueue 刷盘任务 ***********************
             *  todo CommitLog 和 ConsumeQueue 是如何保证一致的？
             *  - 在 Broker 启动时，会对 CommitLog 和 ConsumeQueue 进行恢复，把无效的 CommitLog 和 ConsumeQueue 删除掉，其中 ConsumeQueue 以 CommitLog 为准，保证消息关系正确、一致
             *    @see org.apache.rocketmq.store.DefaultMessageStore#recover(boolean lastExitOK)
             *  - RocketMQ 在运行期间，可以认为消息重放不会存在问题，在重放 ConsumeQueue 会重试 30 次，即使这期间失败的话，会将对象设置为不可写
             *  - ReputMessageService 任务会马不停蹄的做消息重做
             *  总的来说，RocketMQ 在 Broker 启动时就把一致性问题解决了，运行期间不会发生问题，即使断电异常，下次启动又会校验、恢复。
             * */
            // todo 设置从 CommitLog 哪个物理偏移量开始转发消息给 ConsumeQueue 和 Index 文件
            this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);

            // todo 启动 CommitLog 转发到 ConsumeQueue 、 index 文件的任务，该任务基本不停歇的执行
            this.reputMessageService.start();

            /**
             *  1. Finish dispatching the messages fall behind, then to start other services.
             *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
             */
            while (true) {
                // 等待剩余需要重放的消息，发送完成
                if (dispatchBehindBytes() <= 0) {
                    break;
                }
                Thread.sleep(1000);
                log.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
            }
            // 主要用于恢复 CommitLog 中维护的 HashMap<String /* topic-queueId*/,Long /* offSet */> topicQueueTable
            this.recoverTopicQueueTable();
        }

        // todo 没有开启 DLedgerCommitLog ，可以启动 高可用服务(HAService)、延时消息处理任务(ScheduleMessageService)
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService.start();
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }

        //todo 启动 ConsumeQueue 刷盘任务
        this.flushConsumeQueueService.start();

        //todo 启动 CommitLog 刷盘任务
        this.commitLog.start();

        //todo 启动 消息存储统计服务
        this.storeStatsService.start();

        this.createTempFile();

        // todo 开启系列定时任务，其中包含用来清理过期文件的定时任务
        this.addScheduleTask();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();
            this.diskCheckScheduledExecutorService.shutdown();
            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }
            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    /**
     * 单条消息检查
     *
     * @param msg
     * @return
     */
    private PutMessageStatus checkMessage(MessageExtBrokerInner msg) {
        // 消息主题长度检查
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        // 消息属性长度检查
        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }
        return PutMessageStatus.PUT_OK;
    }

    /**
     * 批量消息检查
     * @param messageExtBatch
     * @return
     */
    private PutMessageStatus checkMessages(MessageExtBatch messageExtBatch) {
        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + messageExtBatch.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        // 消息属性检查，默认 4mb
        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        return PutMessageStatus.PUT_OK;
    }

    /**
     * 检查存储服务状态
     *
     * @return
     */
    private PutMessageStatus checkStoreStatus() {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("broke role is slave, so putMessage is forbidden");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        /**
         * 当前服务是否可写
         */
        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("the message store is not writable. It may be caused by one of the following reasons: " +
                    "the broker's disk is full, write to logic queue error, write to index file error, etc");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        } else {
            this.printTimes.set(0);
        }

        /**
         * 判断 OS 是否繁忙
         */
        if (this.isOSPageCacheBusy()) {
            return PutMessageStatus.OS_PAGECACHE_BUSY;
        }
        return PutMessageStatus.PUT_OK;
    }

    /**
     * 异步存储消息
     *
     * @param msg MessageInstance to store
     * @return
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        // 检查当前存储服务状态
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        // 检查消息
        PutMessageStatus msgCheckStatus = this.checkMessage(msg);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }

        // 异步消息存储
        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        // todo putResultFuture 执行完成会回调该方法，但是执行线程不会等待，直接返回
        putResultFuture.thenAccept((result) -> {
            // 统计消耗时间
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("putMessage not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
            }
        });

        return putResultFuture;
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> resultFuture = this.commitLog.asyncPutMessages(messageExtBatch);

        resultFuture.thenAccept((result) -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
            }

            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            // 记录写入 CommitLog 失败次数
            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
            }
        });

        return resultFuture;
    }

    /**
     * 消息存储
     * 说明：存储消息封装，最终存储是需要 CommitLog 实现的
     *
     * @param msg Message instance to store
     * @return
     */
    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        // 检查当前存储服务状态
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return new PutMessageResult(checkStoreStatus, null);
        }

        // 检查消息
        PutMessageStatus msgCheckStatus = this.checkMessage(msg);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return new PutMessageResult(msgCheckStatus, null);
        }

        // todo 消息写入开始计时
        long beginTime = this.getSystemClock().now();

        // todo 将消息写入 CommitLog ，具体实现类 CommitLog
        PutMessageResult result = this.commitLog.putMessage(msg);

        // todo 消息写入计时结束，计算本次消息写入耗时
        long elapsedTime = this.getSystemClock().now() - beginTime;
        if (elapsedTime > 500) {
            log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
        }

        // 记录相关统计信息
        this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

        // 记录写 CommitLog 失败次数
        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return new PutMessageResult(checkStoreStatus, null);
        }

        PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return new PutMessageResult(msgCheckStatus, null);
        }

        long beginTime = this.getSystemClock().now();
        PutMessageResult result = this.commitLog.putMessages(messageExtBatch);
        long elapsedTime = this.getSystemClock().now() - beginTime;
        if (elapsedTime > 500) {
            log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
        }

        this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

        if (null == result || !result.isOk()) {
            this.storeStatsService.getPutMessageFailedTimes().incrementAndGet();
        }

        return result;
    }

    /**
     * 判断操作系统 PageCache 是否繁忙，如果忙，返回 true
     *
     * @return busy-true NoBusy-false
     */
    @Override
    public boolean isOSPageCacheBusy() {
        // 消息写入 CommitLog 文件时上锁的时间
        /** @see CommitLog#putMessage(MessageExtBrokerInner msg) */
        long begin = this.getCommitLog().getBeginTimeInLock();

        // 一次消息追加过程，截止到当前持有锁的时长，完成后重置为 0
        // 即往内存映射文件中或 PageCache 追加消息直到现在所耗费的时间
        long diff = this.systemClock.now() - begin;

        //如果一次消息追加过程的耗时超过了 Broker 配置文件中 osPageCacheBusyTimeOutMills ，默认 1000 即 1s，则认为 PageCache 繁忙
        return diff < 10000000
            && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    /**
     * 获取消息
     * @param group Consumer group that launches this query. 消费组名称
     * @param topic Topic to query.  消息主题
     * @param queueId Queue ID to query. 消息队列id
     * @param offset Logical offset to start from. 拉取的消息队列逻辑偏移量
     * @param maxMsgNums Maximum count of messages to query. 一次拉取的消息条数，默认为 32
     * @param messageFilter Message filter used to screen desired messages. 消息 过滤器 - 如：根据 tag 过滤
     * @return
     */
    public GetMessageResult getMessage(final String group,
                                       final String topic,
                                       final int queueId,
                                       final long offset,
                                       final int maxMsgNums,
                                       final MessageFilter messageFilter) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        long beginTime = this.getSystemClock().now();

        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;

        // 1. 拉取消息的队列偏移量
        long nextBeginOffset = offset;
        // 当前消息消费队列 ConsumeQueue 最小逻辑偏移量
        long minOffset = 0;
        // 当前消息消费队列 ConsumeQueue 最大逻辑偏移量
        long maxOffset = 0;

        // 2. 拉取消息的结果
        GetMessageResult getResult = new GetMessageResult();

        // 3. 获取 CommitLog 文件中的最大物理偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        // 4. 根据消息主题、消息队列id 获取 ConsumeQueue
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            // 选中的消息消费队列的最小编号 todo ConsumeQueue 的最小逻辑偏移量
            minOffset = consumeQueue.getMinOffsetInQueue();
            // 选中的消息消费队列的最大编号 todo ConsumeQueue 的最大逻辑偏移量
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            // todo 5.根据需要拉取消息的偏移量 与 ConsumeQueue 中最小，最大逻辑偏移量进行对比，并修正拉取消息偏移量
            // 队列中没有消息
            // 1) 如果是主节点，或者是从节点但开启了 OffsetCheckSlave 的话，下次从头开始拉取，即 0
            // 2) 如果是从节点，并且没有开启 OffsetCheckSlave 的话，则使用原先的 Offset，因为考虑到主从同步延迟的因素，导致从节点 ConsumeQueue 并没有同步到数据
            // OffsetCheckSlave 设置为 false 保险点，当然默认该值为 false，返回状态码：NO_MESSAGE_IN_QUEUE
            if (maxOffset == 0) {
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);

                // 表示要拉取的偏移量小于队列的最小偏移量
                // 1) 如果是主节点，或从节点开启了 OffsetCheckSlave，设置下一次拉取的偏移量为 minOffset
                // 2) 如果是从节点，但没有开启 OffsetCheckSlave，则保持原先的 Offset ，但是这样的话，不是一直无法拉取消息，返回状态码：OFFSET_TOO_SMALL
            } else if (offset < minOffset) {
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);

                // 待拉取偏移量为队列的最大偏移量，表明已经超出了一个，返回状态码：OFFSET_OVERFLOW_ONE
                // 下次拉取消息的偏移量扔为 Offset ，保持不变
                // todo offset < (MappedFile.getFileFromOffset + MappedFile.getReadPosition) / 20，因为写指针停留的位置是下一次开始写入的位置
            } else if (offset == maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);

                // 待拉取偏移量大于队列的最大偏移量，表明已经超出了，返回状态码：OFFSET_OVERFLOW_BADLY
                // 如果为 从节点 && 没有开启 OffsetCheckSlave，则设置下一次拉取的偏移量为 原偏移量，这是正常的，等待消息到达服务器
                // 如果是主节点 或者从节点开启 OffsetCheckSlave，表示 Offset 是一个非法偏移量，
                //   1) 如果 minOffset == 0，则设置下一次拉取的偏移量为 0，从头开始拉取；
                //   todo 从头开始拉取，有可能存在消息重复消费的问题？
                //   2) 如果 minOffset != 0，则设置下一次拉取的偏移量为 maxOffset，从 maxOffset 开始拉取；
                //   todo 从 maxOffset 开始拉取，有可能存在消息丢失的问题？拉取完消息，进行第二次拉取时，需要重点看下这些状态下，应该还有第二次消息偏移量的修正处理
            } else if (offset > maxOffset) {
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }

                // Offset > minOffSet && Offset < MaxOffSet ，正常情况下
            } else {

                // 6. 从 ConsumeQueue 中获取消息当前逻辑偏移量，对应在 CommitLog 中的物理偏移量、消息大小、消息tagCode
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);

                if (bufferConsumeQueue != null) {
                    try {

                        // 7. 初始化基本变量
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        // 下一个 MappedFile 文件的起始物理偏移量
                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        // 根据传入的拉取消息的最大数量，计算要过滤的消息索引长度，即最大过滤消息字节数
                        // todo 这里为啥不直接 maxFilterMessageCount = maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE ?
                        //  因为拉取到的消息可能满足过滤条件，导致拉取的消息小于 maxMsgNums；
                        //  而且一定会返回 maxMsgNums 条消息吗？不一定返回，因为这里是尽量返回 maxMsgNums 条消息。
                        int i = 0;
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);


                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();

                        // 从读取的消息索引，进行逐条遍历，直到达到预期的消息数量，从遍历条件：i += ConsumeQueue.CQ_STORE_UNIT_SIZE 可以看出
                        // todo 注意：即使没有达到要拉取的最大消息数，但是获取的消息索引，已经遍历完了，那么就不用管，等待下次拉取消息请求即可。
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {

                            // 读取一个消息索引的 3 个属性：在CommitLog 中的物理偏移量、消息大小、消息tagCode
                            // 在CommitLog 中的物理偏移量
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            // 消息大小
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            // todo 消息tagCode (作为过滤条件)
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            // 当前拉取的物理偏移量，即拉取到的最大 Offset
                            // 根据消息顺序拉取的基本原则，可以基本预测下次开始拉取的物理偏移量将大于该值，并且就在这个值附近
                            maxPhyOffsetPulling = offsetPy;

                            // 如果读取到的消息索引，在 CommitLog 中物理偏移量小于下一个 MappedFile 文件的起始物理偏移量，则跳过，继续下一条
                            // 因为 OffsetPay 小于 nextPhyFileStartOffset 时，意味着对应的 MappedFile 已经移除，所以直接 continue 跳过
                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            // 校验当前物理偏移量 OffsetPy 对应的消息是否还在内存中，即校验 CommitLog 是否需要硬盘，因为一般消息过大是无法全部放在内存
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            // todo 本次是否已经拉取到足够的消息，如果足够，则跳出循环
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(), isInDisk)) {
                                break;
                            }

                            // todo 根据偏移量拉取消息索引后，根据 ConsumeQueue 条目进行过滤，如果不匹配则直接跳过该消息，继续拉取下一条消息
                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                        tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            // todo 执行消息过滤，
                            //      如果符合过滤条件，则直接进行消息拉取，
                            //      如果不符合过滤条件，则进入继续执行逻辑，如果最终符合条件，则将该消息添加到拉取结果中
                            if (messageFilter != null
                                    // todo 根据消息的 tagCode 匹配当前的消息索引
                                && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                // 不符合过滤条件，不拉取消息，继续遍历下一个消息索引
                                continue;
                            }

                            // todo 注意：Tag 过滤不需要访问 CommitLog 数据，保证了消息过滤的高效；
                            //  此外，即使存在 Tag 的哈希冲突，也没有关系，因为在 Consume 端还会进行一次过滤以修正，保证万无一失。

                            // 有 消息在CommitLog的物理偏移量、消息大小，就可以在 CommitLog 文件中查找消息
                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {

                                // 从 CommitLog 中找不到消息，则说明对应的 MappedFile 已经删除，计算下一个 MappedFile 的起始位置
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                // 如果该物理偏移量没有找到正确的消息，则说明已经到了文件末尾，下一次切换到下一个 CommitLog 文件读取
                                // 返回下一个文件的起始物理偏移量
                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            // todo 从 CommitLog(全量消息)再次过滤，ConsumeQueue 中只能处理 TagCode 模式的过滤，SQL92 模式的无法在 ConsumeQueue 中过滤
                            // 因为 SQL92 需要依赖消息中的属性，故在这里再做一次过滤，如果消息符合条件，则加入到拉取结果中
                            if (messageFilter != null
                                && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...释放一次 MappedFile 的引用，此处 MappedFile 是 CommitLog 的文件
                                selectResult.release();
                                continue;
                            }

                            // 消息传输次数 + 1
                            this.storeStatsService.getGetMessageTransferedMsgCount().incrementAndGet();

                            // 添加消息
                            getResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;

                            // 重置下一个文件的物理偏移量
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        // 统计剩余可拉取消息字节数
                        if (diskFallRecorded) {
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }

                        // todo 计算下次从当前 ConsumeQueue 中拉取消息的逻辑偏移量
                        // 消费几个消息，就往前推进几个消息
                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        // todo 建议下次拉取消息从哪个 Broker 拉取
                        // 当前未被拉取到消费端的 CommitLog 中的消息长度
                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        // RocketMQ 消息常驻内存大小： 40% * (机器物理内存)；
                        // 超过该大小，RocketMq 会将旧的消息换回磁盘，如果要访问不在 PageCache 的消息，则需要从磁盘中获取
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE // 所在服务器的总内存大小
                                * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));

                        /**
                         * todo 触发下次从从服务器拉取的条件，剩余可拉取的消息大小 > RocketMQ 消息常驻内存大小
                         * 即 diff > memory 表示下次可拉取的消息大小 超出了 RocketMQ 常驻内存的大小，反应了主服务器的繁忙，建议从从服务器拉取消息。
                         * todo 该属性在 {@link org.apache.rocketmq.broker.processor.PullMessageProcessor#processRequest} 处使用
                         */
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {
                        // 释放 BufferConsumeQueue 对 MappedFile 的引用；此处的 MappedFile 是 ConsumeQueue 的文件
                        bufferConsumeQueue.release();
                    }

                    // todo 从指定偏移量没有获取到对应的消息索引，那么计算 ConsumeQueue 从 Offset 开始的下一个 MappedFile 文件的起始物理偏移量对应的位置
                } else {
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                        + maxOffset + ", but access logic queue failed.");
                }
            }
            // todo 没有对应的消息队列
        } else {
            // 没有对应的消息队列，下次从哪个逻辑偏移量开始拉取；
            // 主节点或从节点开启 OffsetCheckSlave ，那么从头开始拉取
            // 从节点但没有开启 OffsetCheckSlave ，那么还保持原先的 Offset 进行拉取
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        // 如果拉取到消息，记录统计信息：消耗时间，拉取到的消息/未拉取到的消息次数
        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().incrementAndGet();
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().incrementAndGet();
        }
        long elapsedTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

        // todo 设置返回结果，状态、下次拉取的逻辑偏移量、ConsumeQueue最大逻辑偏移量、ConsumeQueue最小逻辑偏移量
        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    /**
     * 根据偏移量从 CommitLog 中查找对应的消息，具体过程：
     * 1. 根据偏移量读取对应消息的长度，因为 CommitLog 前 4 个字节存储的是消息的长度
     * 2. 知道了消息的长度，就可以获取消息了
     * @param commitLogOffset physical offset. 物理偏移量
     * @return
     */
    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE 消息的长度
                int size = sbr.getByteBuffer().getInt();

                // 2. 根据物理偏移量和消息长度获取消息
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    /**
     * 获取 CommitLog 的物理存储路径
     * @return
     */
    private String getStorePathPhysic() {
        String storePathPhysic = "";
        // 如果启动 DLegerCommitLog
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
            // DLegerCommitLog 的存储路径
            storePathPhysic = ((DLedgerCommitLog)DefaultMessageStore.this.getCommitLog()).getdLedgerServer().getdLedgerConfig().getDataStorePath();
        } else {
            // CommitLog 默认存储路径： /Users/huanglibao/store/commitlog
            storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        }
        return storePathPhysic;
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(physicRatio));

        }

        {

            String storePathLogics = StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return getStoreTime(result);
        }

        return -1;
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    /**
     * 将消息 data 追加到 CommitLog 内存映射文件中
     * @param startOffset starting offset.
     * @param data data to append.
     * @return
     */
    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data);
        // 如果追加成功，则唤醒 ReputMessageService 实时将消息转发给 ConsumeQueue 和 Index 文件
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.excuteDeleteFilesManualy();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)
                    && !topic.equals(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                        cq.getTopic(),
                        cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                    this.brokerStatsManager.onTopicDeleted(topic);
                }

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                            nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId(),
                            nextQT.getValue().getMaxPhysicOffset(),
                            nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                            topic,
                            nextQT.getKey(),
                            minCommitLogOffset,
                            maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
        SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    /**
     * 检查 Topic 下 queueId 消费队列 的 ConsumerOffset 逻辑偏移量对应的消息是否在内存中
     *  true--不在，false--在
     * @param topic topic.
     * @param queueId queue ID.
     * @param consumeOffset consume queue offset.
     * @return
     */
    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        // 消息的最大物理偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        // 当前消息的物理偏移量
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 根据 Topic 和 QueueId 获取 消费队列
     * @param topic
     * @param queueId
     * @return
     */
    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        // 根据 Topic 获取所有的消费队列
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);

        // 如果没有，则创建一个 ConcurrentMap<Integer, ConsumeQueue> 并缓存起来
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        // 获取 queueId 对应的 消费队列
        ConsumeQueue logic = map.get(queueId);

        // 如果没有，则创建一个 消费队列 并缓存起来
        if (null == logic) {
            // 创建消费队列，注意一个逻辑：队列包含多个物理文件
            ConsumeQueue newLogic = new ConsumeQueue(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    /**
     * 下一个获取队列 Offset 修正
     *修正条件： 主节点或者从节点开启校验 Offset 开关
     *
     * @param oldOffset 老队列 Offset
     * @param newOffset 新队列 Offset
     * @return
     */
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;

        // 如果是主节点或者从节点开启校验 Offset 开关，则修正
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    /**
     * 校验当前物理偏移量 offsetPy 对应的消息是否还在内存中
     * 即 maxOffsetPy - offsetPy > memory 的话，说明 offsetPy 这个偏移量的消息已经从内存置换到磁盘中了
     * @param offsetPy CommitLog 中消息对应的物理偏移量
     * @param maxOffsetPy CommitLog 中最大的物理偏移量
     * @return
     */
    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        // 驻内存的消息大小： 物理内存总大小 * 消息存储内存的比例
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));

        // 是否超过驻内存的大小
        return (maxOffsetPy - offsetPy) > memory;
    }

    /**
     * 判断本次拉取任务是否完成
     *
     * @param sizePy       当前消息字节长度
     * @param maxMsgNums   一次拉取消息条数 默认为 32
     * @param bufferTotal  截止到当前，已拉取消息字节总长度，不包含当前消息
     * @param messageTotal 截止到当前，已拉取消息的个数
     * @param isInDisk     当前消息是否存在于磁盘中
     * @return
     */
    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        // 达到预期的拉取消息条数
        if (maxMsgNums <= messageTotal) {
            return true;
        }

        // 消息在磁盘上
        if (isInDisk) {
            // 已拉取消息字节数 + 待拉取消息的长度 ，达到了磁盘消息的传输上限(默认 64kb)
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            // 已拉取消息条数 + 待拉取消息条数，达到了磁盘消息的传输数量上限(默认 8)
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }

        // 消息不在磁盘上
        } else {
            // 已拉取消息字节数 + 待拉取消息的长度 ，达到了内存消息的传输上限(默认 256kb)
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            // 已拉取消息条数 + 待拉取消息条数，达到了内存消息的传输数量上限(默认 32)
            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    private void addScheduleTask() {

        /**
         * todo 周期性清理 CommitLog、ConsumeQueue 、Index 过期文件，默认每隔 10s 检查一次过期文件
         * 说明：
         * 1. RocketMQ 操作 CommitLog、ConsumeQueue 文件，都是基于内存映射方法，并在Broker 启动时，会加载 CommitLog 和 ConsumeQueue 目录下的所有文件
         * 2. 为了避免内存和磁盘的浪费，不可能将消息永久的存储在消息服务器上，所以需要一种机制来删除已过期的文件。
         * 3. RocketMQ 写入 CommitLog 和 ConsumeQueue 文件是顺序写入，所有的写操作全部落在最后一个 CommitLog 和 ConsumeQueue 文件上，之前的文件在下一个文件创建后，将不会再被写入消息，也就是更新
         * 4. RocketMQ 清除过期文件的方法是：
         *    4.1 如果非当前写文件在一定时间间隔内，没有被再次更新，则认为是过期文件，可以被删除，默认每个文件的过期时间为 72 小时，可以通过在 Broker 配置文件中设置 FileReservedTime 来改变过期时间，单位为小时
         *    4.2 清理过期文件时，RocketMQ 不会管这个文件上的消息是否被全部消费
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // todo 定期执行清理过期文件的任务，默认 10s 执行一次，有三种情况会执行清理，具体看流程
                // 主要清理 CommitLog、ConsumeQueue、Index 三种文件，如何定义过期文件，ConsumeQueue、Index 两个文件的判断方式大同小异
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                    + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
        this.diskCheckScheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                DefaultMessageStore.this.cleanCommitLogService.isSpaceFull();
            }
        }, 1000L, 10000L, TimeUnit.MILLISECONDS);
    }

    /**
     * todo 过期文件清理
     */
    private void cleanFilesPeriodically() {
        // 1. CommitLog 文件清理
        this.cleanCommitLogService.run();

        // 2. ConsumeQueue 文件清理 && Index 文件清理
        // todo 为什么要先清理 CommitLog ，因为清理 ConsumeQueue 和 Index 文件，依赖于 CommitLog 文件(以消息最小物理偏移量为准)
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    /**
     * 加载 ConsumeQueue 队列
     * @return
     */
    private boolean loadConsumeQueue() {
        // 封装 $user.home/store/consumequeue 文件
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));

        // 获取 $user.home/store/consumequeue 目录下所有子目录，加载进来
        File[] fileTopicList = dirLogic.listFiles();

        if (fileTopicList != null) {

            // 遍历 Topic 级别的目录
            for (File fileTopic : fileTopicList) {

                // 文件夹对应的名 - Topic
                String topic = fileTopic.getName();

                // Topic 文件夹下的列表 - queue 文件列表
                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {

                    // 遍历 queue 文件列表
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            // 获取 queueId
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }

                        // todo 还原当前 queueId 对应的 ConsumeQueue ，这是一个逻辑文件，具体由 MappedFileQueue 管理多个映射文件 MappedFile
                        // 构建 ConsumeQueue，主要初始化：topic、queueId、storePath、MappedFileSize
                        ConsumeQueue logic = new ConsumeQueue(
                            topic,
                            queueId,
                            StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                            this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                            this);
                        // todo 将文件中的 ConsumeQueue 加载到内存中
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    /**
     * 文件恢复
     *
     * @param lastExitOK Broker 是否正常关闭
     */
    private void recover(final boolean lastExitOK) {
        // 1. 恢复消息队列，返回的结果是 ConsumeQueue 中写入的消息的最大物理偏移量
        // 即移除非法的 offset
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();

        // 2. 恢复 CommitLog
        if (lastExitOK) {
            // 正常退出，按照正常逻辑恢复，即移除非法的 offset ，并且进一步移除 ConsumeQueue 的非法 offset
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            // 异常退出，按照异常修复逻辑
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }

        // 3. 修复主题队列
        // todo 恢复 CommitLog 和 ConsumeQueue 文件后，在 CommitLog 实例中存储了每个消息消费队列的逻辑偏移量，这个用于处理消息发送和消息重放
        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    /**
     * 更新存储服务内存中的 ConsumeQueue
     * @param topic
     * @param queueId
     * @param consumeQueue
     */
    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    /**
     * 恢复并返回 ConsumeQueue 中写入消息的最大物理偏移量
     * @return
     */
    private long recoverConsumeQueue() {
        long maxPhysicOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                // 恢复 ConsumeQueue ，只保留有效条目
                logic.recover();
                // 获取并记录 ConsumeQueue 的最大物理偏移量
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }

        return maxPhysicOffset;
    }

    /**
     * 恢复主题队列
     */
    public void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        // 获取 CommitLog 的最小物理偏移量
        long minPhyOffset = this.commitLog.getMinOffset();

        // 遍历 Topic 下消息队列 ConsumeQueue
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                // 更新 Topic-queueid 的。逻辑偏移量
                table.put(key, logic.getMaxOffsetInQueue());
                // logic 的最小逻辑偏移量
                logic.correctMinOffset(minPhyOffset);
            }
        }
        // 更新 CommitLog 中的 Topic-queueid 对应的逻辑偏移量
        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    /**
     * 执行调度请求
     * 1. 非事务消息或事务提交消息，建立消息位置到 ConsumeQueue
     * 2. 建议索引信息到 Index 文件
     * @param req
     */
    public void doDispatch(DispatchRequest req) {
        // 转发 CommitLog 日志，主要从 CommitLog 到 ConsumeQueue 、 Index 文件
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    /**
     * 将消息的位置信息放入到 ConsumeQueue 中
     * @param dispatchRequest
     */
    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        // 根据消息的 Topic 和 QueueId，查找对应的 ConsumeQueue
        // todo 因为每一个消息主题下每一个消息消费队列对应一个文件
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        // 建立消息位置信息到 ConsumeQueue
        cq.putMessagePositionInfoWrapper(dispatchRequest);
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    /**
     * 处理延时消息任务的启动与否
     *
     * @param brokerRole
     */
    @Override
    public void handleScheduleMessageService(final BrokerRole brokerRole) {
        if (this.scheduleMessageService != null) {
            // 如果当前 Broker 是从节点，则关闭
            if (brokerRole == BrokerRole.SLAVE) {
                this.scheduleMessageService.shutdown();
                // 如果当前 Broker 是主节点，则启动
            } else {
                this.scheduleMessageService.start();
            }
        }

    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.availableBufferNums();
    }

    /**
     * 获取消息堆外内存池是否还有堆外内存可用
     *
     * @return
     */
    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    /**
     * CommitLog 对应的派发器，用于构建 ConsumeQueue
     * todo 特别说明：
     *  对于事务消息，只有提交后的事务消息，才会生成 ConsumeQueue
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            // 获取消息的类型
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                case MessageSysFlag.TRANSACTION_NOT_TYPE: // 非事务消息
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE: // 事务消息提交
                    // 非事务消息 或 事务提交消息，建立消息位置信息到 ConsumeQueue
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    /**
     * CommitLog 对应的派发器，用于建立 index 文件
     * todo 特别说明：
     *  对于构建索引信息到 IndexFile ，无需区分消息类型
     *
     */
    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            // 如果开启使用消息索引
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    /**
     * CommitLog 过期文件清理任务
     */
    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;

        /*---------------------------------------RocketMQ 提供了两个与磁盘空间使用率相关的系统级参数，如下所示： ----------------------------------------------------*/

        /**
         * 通过系统参数设置，默认值为 0.90。如果磁盘分区使用率超过该阈值，将设置磁盘为不可写，此时会拒绝写入新消息，，并且立即启动文件删除操作
         */
        private final double diskSpaceWarningLevelRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        /**
         * 通过系统参数设置，默认值为 0.85。如果磁盘分区使用率超过该阈值，建议立即执行文件删除操作，但是不会拒绝写入新消息
         */
        private final double diskSpaceCleanForciblyRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));

        /*---------------------------------------RocketMQ 提供了两个与磁盘空间使用率相关的系统级参数，如上所示： ----------------------------------------------------*/
        private long lastRedeleteTimestamp = 0;

        private volatile int manualDeleteFileSeveralTimes = 0;

        /**
         * 是否立即清理文件，根据磁盘使用情况，更新该值
         */
        private volatile boolean cleanImmediately = false;

        /**
         * 设置手动删除
         */
        public void excuteDeleteFilesManualy() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        /**
         * 清理 CommitLog 过期文件，整个执行过程分为两个大的步骤
         * 1. 尝试删除过期文件
         * 2. 重试删除被 hold 住的文件(由于被其他线程引用，在第一步没有被删除的文件)，再试一次
         */
        public void run() {
            try {
                // 清理过期文件
                this.deleteExpiredFiles();

                // 重试没有删除成功的过期文件(在拒绝被删除保护期 destroyMapedFileIntervalForcibly 内)
                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * 清理过期文件
         * 默认过期时间为72小时即3天，除了我们自动清理过期文件，下面几种情况也会自动清理，无论文件是否被消费过，都会被清理
         * 1. 默认是凌晨 4 点，自动清理过期文件
         * 2. 文件过期，磁盘空间利用率超过 75% ，无论是否达到清理时间，都会自动清理过期文件
         * 3. 磁盘使用率达到清理阈值，默认 85% 后，按照设定好的清理规则(默认是时间最早的)清理文件，无论是否过期
         * 4. 磁盘占用率达到默认 90% ，Broker 拒绝消息写入
         */
        private void deleteExpiredFiles() {
            int deleteCount = 0;

            // 文件保存时长(从最后一次更新时间到现在)，默认 72小时，如果超过了该时间，则认为是过期文件
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();

            // 删除物理文件的间隔时间，在一次清理过程中，可能被需要删除的文件不止一个，该值指定了删除了一个文件后，需要等待多长时间，才能再删除一个文件
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();

            // 在清除过期文件时，如果该文件被其他线程占用(引用次数大于 0，比如读取消息)，此时会阻止此次删除任务，同时在第一次试图删除该文件时记录当前时间戳。
            // 该值表示第一次拒绝删除之后，能保留文件的最大时间，在此时间内，同样地，可以被拒绝删除，直到超过该时间，会将引用次数设置为负数，文件将被强制删除
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            /**
             * todo
             *  ----------------------------------RocketMQ 在如下三种情况下任意满足其中一种情况，都将执行删除文件操作 ------------------------------------
             *  1. 到了删除文件的时间点，RocketMQ 通过设置 deleteWhen 设置在一天中某个固定时间执行一次删除过期文件，默认为凌晨 4 点
             *  2. 判断磁盘空间是否充足，如果不充足，则返回 true，表示应该触发过期文件删除操作
             *  3. 预留，手工触发删除：通过调用 excuteDeleteFilesManualy 方法，手动触发删除
             */

            // 1. todo 清理时间到达，默认为每天凌晨 4点
            boolean timeup = this.isTimeToDelete();
            // 2. todo 磁盘空间是否要满了，占用率默认为 75 %
            boolean spacefull = this.isSpaceToDelete();
            // 3. todo 手动可删除次数 大于 0
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            if (timeup || spacefull || manualDelete) {

                // 如果是手动删除，那么递减可手动删除次数
                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                // 是否立即删除
                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                    fileReservedTime,
                    timeup,
                    spacefull,
                    manualDeleteFileSeveralTimes,
                    cleanAtOnce);

                // 将小时 换算成 毫秒
                fileReservedTime *= 60 * 60 * 1000;

                // todo 开始清理 CommitLog 文件，从最开始到倒数第二个文件的范围内清理
                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                    destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        /**
         * 重试没有删除成功的过期文件
         * todo 说明，尝试删除的是第一个 MappedFile
         */
        private void redeleteHangedFile() {
            // 获取重新删除挂起文件的时间间隔 默认 120s
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            // 如果当前时间 - 上次重试时间戳 大于 120s，则执行删除挂起文件
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;

                // 第一次删除被拒绝后，文件能保留的最大时间
                int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

                // todo 尝试 删除第一个 文件
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        /**
         * 时间是否到达，默认凌晨 4点
         * @return
         */
        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        /**
         * 磁盘空间是否充足
         *
         * @return
         */
        private boolean isSpaceToDelete() {

            // diskMaxUsedSpaceRatio 表示 CommitLog、ConsumeQueue 文件所在磁盘分区的最大使用量，默认 75%，如果超过该值，需要立即清理过期文件
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            // 是否需要立即执行清除文件的操作，与文件过期时间无关了
            cleanImmediately = false;

            {
                /**
                 * todo 处理 CommitLog 过期文件是否立即执行的逻辑
                 */
                // 当前 CommitLog 文件所在磁盘分区的使用率(CommitLog 已经占用的存储容量 / CommitLog 目录所在磁盘分区总的存储容量)
                double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());

                // todo 判断磁盘是否可用，用当前已使用物理磁盘 physicRatio 与 diskSpaceWarningLevelRatio、diskSpaceCleanForciblyRatio 进行判断
                //  如果当前磁盘使用率达到上述阈值，将返回 true ，表示磁盘已满，需要进行文件删除

                // 如果当前磁盘使用率大于 diskSpaceWarningLevelRatio 0.90 ，应立即启动过期文件删除操作
                if (physicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + physicRatio + ", so mark disk full");
                    }

                    // 设置立即启动文件删除
                    cleanImmediately = true;

                    // 如果当前磁盘使用率 大于 diskSpaceCleanForciblyRatio 0.85 建议立即启动过期文件删除操作
                } else if (physicRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;

                    // 如果当前磁盘使用率 小于 diskSpaceCleanForciblyRatio 0.85 将恢复磁盘读写
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + physicRatio + ", so mark disk ok");
                    }
                }

                // 如果当前磁盘使用率 小于 0 或者 大于 ratio 0.75，表示磁盘空间可能很快不足，需要立即清理过期文件
                if (physicRatio < 0 || physicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, " + physicRatio);
                    return true;
                }
            }

            /**
             * todo 处理 ConsumeQueue 过期文件是否立即执行的逻辑，同 CommitLog 过期文件逻辑相同
             */

            {
                String storePathLogics = StorePathConfigHelper
                    .getStorePathConsumeQueue(DefaultMessageStore.this.getMessageStoreConfig().getStorePathRootDir());
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            // 默认磁盘空间，没有满
            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }
        public boolean isSpaceFull() {
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathPhysic());
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            if (physicRatio > ratio) {
                DefaultMessageStore.log.info("physic disk of commitLog used: " + physicRatio);
            }
            if (physicRatio > this.diskSpaceWarningLevelRatio) {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskok) {
                    DefaultMessageStore.log.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
                }

                return true;
            } else {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();

                if (!diskok) {
                    DefaultMessageStore.log.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
                }

                return false;
            }
        }
    }

    /**
     * ConsumeQueue 过期文件删除任务 && Index 文件删除任务
     */
    class CleanConsumeQueueService {
        /**
         * ConsumeQueue 最大的物理偏移量
         */
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        /**
         * todo 根据 CommitLog 最小的消息物理偏移量去删除无用的 ConsumeQueue 文件
         */
        private void deleteExpiredFiles() {
            // 删除 ConsumeQueue 文件的时间间隔为 100ms
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            // 获取 CommitLog 最小的物理偏移量
            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();

            // 如果 CommitLog 最小的物理偏移量 > lastPhysicalMinOffset，则执行删除 ConsumeQueue 文件 && Index 文件的操作
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                // 获取内存中的 ConsumeQueue
                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    // 遍历所有的 ConsumeQueue 文件
                    for (ConsumeQueue logic : maps.values()) {
                        // todo 以 CommitLog 文件的最小物理偏移量为标准，从第一个文件，遍历到倒数第二个文件，
                        //   以每个文件中最后一个消息索引条目中记录的物理偏移量，与 CommitLog 文件最小物理偏移量进行比较，
                        //   如果比 CommitLog 最小的物理偏移量小，说明文件过期，则删除该文件
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        // 删除一个 ConsumeQueue 暂停一下
                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                // todo 根据 CommitLog 最小物理偏移量 删除无效 Index 文件
                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    /**
     * ConsumeQueue 刷盘任务
     * todo 说明
     *  1. 刷盘 ConsumeQueue 类似 CommitLog 类似，但是 ConsumeQueue 只有异步刷盘
     *  2. ConsumeQueue 刷盘任务基本和 CommitLog 刷盘任务逻辑一致，可参考 CommitLog 的刷盘逻辑
     * 该任务在 DefaultMessageStore#start() 方法中启动
     * @see CommitLog.FlushRealTimeService
     *
     * <p>
     * flush ConsumeQueue
     */
    class FlushConsumeQueueService extends ServiceThread {
        // 重试次数
        private static final int RETRY_TIMES_OVER = 3;

        // 最后一次刷盘时间戳
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {

            // ConsumeQueue 刷盘时，最少刷盘页数
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            // retryTimes == RETRY_TIMES_OVER 进行强制 Flush ，主要用于 shutdown 时
            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            // 当时间间隔满足 flushConsumeQueueThoroughInterval 时，即使写入的数量不足 flushConsumeQueueLeastPages 也会进行 Flush
            long logicsMsgTimestamp = 0;
            // 刷盘时间间隔
            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            // 如果当前时间 > 最后一次刷盘时间戳 + 刷盘时间间隔，则强制刷盘
            // 每 FlushConsumeQueueThoroughInterval 周期，执行一次 Flush；
            // 因为并不是每次循环都能达到 flushConsumeQueueLeastPages 页数，所以一定周期进行一次 Flush
            // 但是，也不能每次循环都去强制执行 Flush，这样性能较差
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            // 需要 Flush 的 ConsumeQueue
            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            // todo 遍历所有的 ConsumeQueue，进行 刷盘
            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        // todo flush ConsumeQueue
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            // todo 执行 checkPoint 文件的刷盘动作 疑问
            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        /**
         * flush ConsumeQueue 任务
         * <p>
         * 每隔 1s 执行一次 flush
         */
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 每隔 flushIntervalConsumeQueue 秒执行下 flush ConsumeQueue 的任务，默认 1s
                    // 此处并不是每次都能执行 flush 任务的，因为 强制 flush ConsumeQueue 的刷盘时间间隔是 60s 或者 没有达到刷盘需要的最少页数 flushConsumeQueueLeastPages
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    // 等待 interval 时间
                    this.waitForRunning(interval);
                    // 执行 flush ConsumeQueue
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Broker 关闭时，强制刷盘 ConsumeQueue
            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    /**
     * todo：重放消息线程任务
     *  1. 该服务不断生成 消息位置信息 写入到 消息消费队列 ConsumeQueue
     *  2. 该服务不断生成 消息索引 写入到 索引文件(IndexFile)
     *  特别说明
     *  1. reputFromOffset 不断指向下一条消息，生成 ConsumeQueue 和 IndexFile 对应的内容
     *  2. ReputFromOffset 指向 BLANK 时，即文件末尾时，则指向下一个 MappedFile
     *  <p>
     *  RocketMQ 采用专门的线程来根据 CommitLog Offset 来将 CommitLog 转发给 ConsumeQueue 和 IndexFile
     *
     *  @see DefaultMessageStore#start() 方法中启动该任务，Broker 启动时，该任务就会被启动
     */
    class ReputMessageService extends ServiceThread {

        /**
         * 重放消息的 CommitLog 物理偏移量
         * 1. 该值的初始化值是在 Broker 启动是赋予的，是个绝对正确的物理偏移量
         * 2. 随着重放信息的过程，该值会不断推进
         */
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                    DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        /**
         * 剩余需要重放消息字节数
         *
         * @return
         */
        public long behind() {
            // CommitLog 的最大物理偏移量 - 要开始重放消息的 CommitLog 位置
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        /**
         * CommitLog 是否可以重放消息
         * @return
         */
        private boolean isCommitLogAvailable() {
            // 只要重放的位置没有达到 CommitLog 文件最大的物理偏移量，则可以重放消息
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        /**
         * 重放消息逻辑，不断生成 消息位置信息到 ConsumeQueue
         */
        private void doReput() {
            // 如果从 CommitLog 中开始拉取(重放ConsumeQueue)的偏移量小于 CommitLog 最小物理偏移量，则重置为 CommitLog 最小物理偏移量
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                log.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                    this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                // 从有效位置开始
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }

            // todo 是否可以继续重放 CommitLog，原则是重放的偏移量，不能大于 CommitLog 的最大物理偏移量
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                // todo 是否允许重复转发，如果允许重复转发，则重放的偏移量 ReputFromOffSet 也不能大于 CommitLog 的提交指针
                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                    && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 根据指定的物理偏移量，从对应的内存文件中读取 物理偏移量 ~ 该当前内存文件MappedFile中有效数据的最大偏移量的数据
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        // 更新 reputFromOffset 为拉取结果的起始偏移量，即 this.fileFromStart + reputFromOffset % mappedFileSize
                        this.reputFromOffset = result.getStartOffset();

                        // 从返回的 SelectMappedBufferResult 中，一条条的循环读取数据，创建 DispatchRequest 对象
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {

                            // 尝试构建转发请求对象 DispatchRequest 即生成重放消息调度请求，请求里面主要包含一条消息或者文件尾 BLANK 的基本消息
                            // todo 主要是从 NIO ByteBuffer 中，根据 CommitLog 中消息的存储格式，解析出消息的核心属性，其中延迟时间的计算也在这个逻辑中
                            //  注意：生成重放消息调度请求（DispatchRequest），从 SelectMappedBufferResult 中读取一条数据(message) 或者文件尾(BLANK) 的基本信息
                            // todo 怎么做到每次只有一条的呢？result.getByteBuffer() 的读取方法指针(每次按照存储顺序往后移动) && readSize 更新控制
                            DispatchRequest dispatchRequest =
                                DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);

                            // 消息长度
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            // 读取成功，进行该条消息的派发
                            if (dispatchRequest.isSuccess()) {

                                // 如果解析到的消息长度 > 0
                                if (size > 0) {
                                    // todo 转发 DispatchRequest 根据 CommitLog 文件内容实时构建 ConsumeQueue 、Index 文件
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                                    // todo 如果当前 Broker 节点是主节点 && Broker 开启的是长轮询，则通知消息队列有新的消息到达，这样处于等待中拉取消息的请求可以再次拉取消息
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
                                            && DefaultMessageStore.this.messageArrivingListener != null) {

                                        // 在消息拉取长轮询模式下的消息到达监听器
                                        DefaultMessageStore.this.messageArrivingListener.arriving(
                                                            dispatchRequest.getTopic(),
                                                            dispatchRequest.getQueueId(),
                                                  dispatchRequest.getConsumeQueueOffset() + 1, // 消息在消息队列的逻辑偏移量
                                                            dispatchRequest.getTagsCode(),
                                                            dispatchRequest.getStoreTimestamp(),
                                                            dispatchRequest.getBitMap(),
                                                            dispatchRequest.getPropertiesMap());
                                    }

                                    // todo 更新下次重放消息的物理偏移量
                                    this.reputFromOffset += size;

                                    // todo 累计读取大小，判断是否读取完毕以及控制每次读取一条消息
                                    readSize += size;

                                    // 统计信息
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).incrementAndGet();
                                        DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                            .addAndGet(dispatchRequest.getMsgSize());
                                    }

                                    // 消息长度为 0 对应的是文件尾 BLANK，即读取到 MappedFile 文件尾部，跳转指向下一个 MappedFile
                                } else if (size == 0) {
                                    // 更新 reputFromOffset 为下一个 MappedFile 文件的起始物理偏移量
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }

                                // 读取失败
                            } else if (!dispatchRequest.isSuccess()) {
                                // 如果消息长度 > 0 , 更新 reputFromOffset 偏移量，再循环进行下次读取
                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;

                                    // 如果消息长度 <= 0 ，则跳出循环，结束此次派发
                                } else {
                                    doNext = false;
                                    // If user open the dledger pattern or the broker is master node,
                                    // it will not ignore the exception and fix the reputFromOffset variable
                                    // 如果开启 DLegerCommitLog || 当前 Broker 是主节点，则不忽略异常，修复 reputFromOffset 变量
                                    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog() ||
                                        DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                            this.reputFromOffset);
                                        // 更新 reputFromOffset 偏移量 为 结果总大小 - 读取大小
                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }

                    // 结束重放消息位置信息到 ConsumeQueue
                } else {
                    doNext = false;
                }
            }
        }

        /**
         * 重放消息线程任务，每执行一次，休息 1ms 后，继续尝试推送消息到 ConsumeQueue 和 Index 文件
         * 即 不断生成消息位置信息到 ConsumeQueue 和 消息索引文件 Index
         */
        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 每休息 1ms，就继续抢占 CPU 时间，可以认为是实时的了
                    Thread.sleep(1);

                    // 每处理一次 消息派发，休眠 1ms，几乎是一直在转发 CommitLog 中的内容到 ConsumeQueue 和 Index 文件
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
