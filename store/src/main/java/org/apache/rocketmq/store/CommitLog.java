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

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.sun.xml.internal.bind.v2.TODO;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

/**
 * Store all metadata downtime for recovery, data protection reliability
 * 消息主体以及元数据的存储主体，存储 Producer端写入的消息主体内容，消息内容不是定长的。
 * <p>
 *  前置说明：
 *  1. CommitLog : MappedFileQueue : MappedFile = 1 : 1 : N
 *  2. 为什么 CommitLog 文件要设计为固定大小的长度呢？为了使用 内存映射机制
 * <p>
 * 说明
 * 1. 单个 CommitLoglog 的文件大小为 1GB，由多个 CommitLog 文件来存储所有的消息。CommitLog 文件的命令使用存储在该文件中的第一条消息在整个 CommitLog 文件组中的偏移量来命名。
 * 即该文件在整个 CommitLog 文件组中的偏移量来命名的。举例：
 * 例如一个 CommitLog 文件 1024 个字节
 * 第一个文件：00000000000000000000，起始偏移量为 0
 * 第二个文件：00000000001073741824，起始偏移量为 1073741824
 * 2. MappedFile 封装了一个个的 CommitLog 文件，而 MapperFileQueue 就是封装了一个逻辑的 CommitLog 文件，这个 MappedFile 队列中的元素从小到大排列
 * 3. MappedFile 还封装了 jdk 中的 MappedByteBuffer
 * 4. 同步刷盘逻辑：
 *    每次发送消息，消息都直接存储在 MappedFile 的 MappedByteBuffer 然后直接调用 force() 方法刷写到磁盘；
 *    等到 force 刷盘成功后，再返回给调用方 (GroupCommitRequest#waitForFlush) 就是其同步调用的实现。
 * 5. 异步刷盘逻辑：
 *    分为两种，是否开启堆外内存缓冲池，具体配置参数：MessageStoreConfig#transientStorePoolEnable
 *    5.1 transientStorePoolEnable = true
 *    消息在追加时，先放入到 writeBuffer 中，然后定时 Commit 到 FileChannel ；然后定时 Flush。
 *    5.2 transientStorePoolEnable = false
 *    消息追加时，直接存入 MappedByteBuffer(PageCache) 中，然后定时 Flush
 * </p>
 *
 */
public class CommitLog {
    // Message's MAGIC CODE daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // End of file empty MAGIC CODE cbd43194
    // 文件末尾空的 MAGIC CODE
    protected final static int BLANK_MAGIC_CODE = -875286124;
    /**
     * 针对 MappedFile 的封装。可以看作是 .../store/commitlog 文件夹，而 MappedFile 则对应该文件夹下的文件
     */
    protected final MappedFileQueue mappedFileQueue;

    /**
     * 消息存储的核心对象
     */
    protected final DefaultMessageStore defaultMessageStore;

    /**
     * 消息刷盘线程，根据刷盘方式，可能是同步刷盘，也可能是异步刷盘
     */
    private final FlushCommitLogService flushCommitLogService;

    /**
     * 如果开启堆外内存，必须将数据提交到 FileChannel ，该任务就是做这个工作的，以一定的频率提交堆外内存到 FileChannel
     */
    //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
    private final FlushCommitLogService commitLogService;

    /**
     * 追加消息函数
     */
    private final AppendMessageCallback appendMessageCallback;
    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;

    /**
     * todo  topic 下 queue 的逻辑偏移量，类似数组下标(注意不是物理偏移量) ，在 Broker 启动时，会根据 ConsumerQueue 计算获得，之后根据消息进行更新。
     * todo 特别说明：在进行 CommitLog 转发时，对应消息队列的逻辑偏移量就是从这里取的，以及处理消息写入时的队列逻辑偏移量也需要这个缓存。
     *
     * @see DefaultMessageStore#load()
     * @see DefaultMessageStore#recoverTopicQueueTable()
     */
    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);

    /**
     * 提交偏移量
     */
    protected volatile long confirmOffset = -1L;

    /**
     * 上锁的开始时间
     */
    private volatile long beginTimeInLock = 0;

    /**
     * 写入消息时需要申请的锁
     */
    protected final PutMessageLock putMessageLock;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        this.mappedFileQueue = new MappedFileQueue(defaultMessageStore.getMessageStoreConfig().getStorePathCommitLog(),
            defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());
        this.defaultMessageStore = defaultMessageStore;

        // 根据刷盘方式，同步刷盘使用 GroupCommitService
        if (FlushDiskType.SYNC_FLUSH == defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();

            // 异步刷盘，使用 FlushRealTimeService
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }

        /**
         * 使用堆外内存的话，必须将数据提交到 FileChannel ，该任务就是做这个的，以一定的频率提交堆外内存到 FileChannel
         * todo 虽然这里创建了，但是如果没有开启的话，也是没有用的，开启的开关在这里
         * @see CommitLog#start()
         */
        this.commitLogService = new CommitRealTimeService();

        /**
         * 追加消息的回调函数
         */
        this.appendMessageCallback = new DefaultAppendMessageCallback(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
        batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
            @Override
            protected MessageExtBatchEncoder initialValue() {
                return new MessageExtBatchEncoder(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };

        // 异步刷盘，建议使用自旋锁，因为每次消息写入成功后，唤醒等待刷盘的线程即可，锁竞争不激烈
        // 同步刷盘，建议使用可重入锁，因为每次消息写入，都必须等待消息写入成功，才能进行刷盘，锁竞争激烈
        this.putMessageLock = defaultMessageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

    }

    /**
     * 从磁盘加载 CommitLog 文件
     * @return
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    /**
     * todo 开启刷盘线程、消息提交任务线程
     */
    public void start() {
        // 开启同步或异步刷盘任务线程
        this.flushCommitLogService.start();

        // 如果开启使用堆外内存，则开启提交 FillChanel 任务线程
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }

        this.flushCommitLogService.shutdown();
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    /**
     * 删除过期文件
     *
     * @param expiredTime         过期时间戳
     * @param deleteFilesInterval 删除文件时，每删除一个文件后等待多长时间能再次删除的时间
     * @param intervalForcibly    强制删除时间间隔（第一次拒绝删除之后能保留文件的最大时间）
     * @param cleanImmediately    是否立即删除
     * @return
     */
    public int deleteExpiredFile(
        final long expiredTime,
        final int deleteFilesInterval,
        final long intervalForcibly,
        final boolean cleanImmediately
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * Read CommitLog data, use data replication
     * 读取 CommitLog 的数据，使用数据复制的方式
     */
    public SelectMappedBufferResult getData(final long offset) {
        // 如果 Offset == 0 ，没有找到就返回第一个就可以
        return this.getData(offset, offset == 0);
    }

    /**
     * 根据偏移量从 MappedFile 中获取数据
     * @param offset
     * @param returnFirstOnNotFound
     * @return
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        // 获取 MappedFile 文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();

        // 根据给定的 偏移量获取该偏移量所在的物理 MappedFile
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);

        // todo 读取偏移量相关数据
        if (mappedFile != null) {
            // 计算 Offset 在 MappedFile 中的偏移量
            int pos = (int) (offset % mappedFileSize);

            // 获取当前 MappedFile 从传入偏移量到写入范围内容数据
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    /**
     * Broker 正常停止文件恢复：从倒数第 3 个 CommitLog 文件开始恢复，如果不足 3 个文件，则从第一个文件开始恢复
     * When the normal exit, data recovery, all memory data have been flush
     *
     * @param maxPhyOffsetOfConsumeQueue ConsumeQueue 记录消息的最大物理偏移量
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        // 是否验证 CRC
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();

        // 获取 CommitLog 的内存文件列表
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            // 从哪个内存文件开始恢复，文件数 >= 3 时，则从倒数第 3 个文件开始
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);

            // 内存映射文件对应的 Bytebuffer
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

            // 该文件的起始物理偏移量，默认从 CommitLog 中存放的第一个条目开始
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;

            while (true) {
                // 使用 Bytebuffer 逐条消息构建消息请求转发对象，直到出现异常或者当前文件读取完(换下一个文件)
                // todo 使用能否构建消息转发对象作为有效性的标准，因为后续消息要重放到 ConsumerQueue 和 IndexFile ，任何一个异常都是不允许的
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                // 消息大小
                int size = dispatchRequest.getMsgSize();

                // Normal data
                // 数据正常
                if (dispatchRequest.isSuccess() && size > 0) {
                    mappedFileOffset += size;
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                // 来到文件末尾，切换到下一个文件
                else if (dispatchRequest.isSuccess() && size == 0) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                // 文件读取错误
                else if (!dispatchRequest.isSuccess()) {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            // processOffset 代表了当前 CommitLog 有效偏移量
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);

            // 删除有效偏移量后的文件，保留文件中有效数据(本质上：更新 MappedFile 的写指针，提交指针，刷盘指针)
            // 即截断无效的 CommitLog 文件，只保留到 processOffset 位置的有效文件
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            // 清除 ConsumerQueue 冗余数据 [ConsumerQueue 中记录的最大物理偏移量大于 commitLog 中最大有效物理偏移量]
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                // todo 只保留记录物理偏移量 < processOffset 的 MappedFile (本质上：更新 MappedFile 的写指针、提交指针、刷盘指针)
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        } else {
            // Commitlog case files are deleted
            // CommitLog 文件被删除
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    /**
     * 构建消息转发对象
     * @param byteBuffer
     * @param checkCRC
     * @return
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * 构建转发请求对象 DispatchRequest
     *
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
        final boolean readBody) {
        try {

            /********************根据消息在 commitLog 中的存储格式，进行读取 **********************************/

            // 1 TOTAL SIZE 当前消息条目大小
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE 魔数
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }


            byte[] bytesContent = new byte[totalSize];
            // 消息体的 crc 校验码
            int bodyCRC = byteBuffer.getInt();
            // todo 消息消费队列 ID
            int queueId = byteBuffer.getInt();
            // FLAG FLAG 消息标记，RocketMQ 对其不做处理，供应用程序使用
            int flag = byteBuffer.getInt();
            // todo 消息队列逻辑偏移量
            long queueOffset = byteBuffer.getLong();
            // todo 消息在 CommitLog 文件中的物理偏移量
            long physicOffset = byteBuffer.getLong();
            // 消息系统标记，例如是否压缩、是否是事务消息等
            int sysFlag = byteBuffer.getInt();
            // 消息生产者调用消息发送 API 的时间戳
            long bornTimeStamp = byteBuffer.getLong();
            // 消息发送者 IP、端口号
            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }
            // todo 消息存储时间戳
            long storeTimestamp = byteBuffer.getLong();

            // Broker 服务器 IP + 端口号
            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            // todo 消息重试次数
            int reconsumeTimes = byteBuffer.getInt();
            // Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            // 消息体长度
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }
            // Topic 主题存储长度
            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);
            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            // todo 处理附加属性，很重要
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);

                // 解析出附加属性
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                // 取出 keys
                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                // 取出 uniq_key 消息的 msgId
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                // 取出 tags
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                // todo 计算 tag 的 HashCode
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing 消息处理时间
                {
                    // 是否是延时消息，根据附加属性中的 DELAY 值判断
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);

                    // 如果是延迟消息
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                        // 解析延迟级别
                        int delayLevel = Integer.parseInt(t);

                        // 超过最大延迟级别，就获取最大的延迟级别
                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        // todo 如果是延迟消息，tagsCode 存储计划消费时间
                        if (delayLevel > 0) {
                            // todo 计算消息的计划消费时间，延迟级别对应的延迟时间 + 消息存储时间
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel,
                                storeTimestamp);
                        }
                    }
                }
            }

            // 计算消息长度
            int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
            // 校验消息长度
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                    "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                    totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            /**
             * 构建转发请求对象 DispatchRequest
             */
            return new DispatchRequest(
                topic, // 消息主题
                queueId, // 消息消费队列id
                physicOffset, // 消息在 CommitLog 文件中的物理偏移量
                totalSize, // 消息大小
                tagsCode, // 消息的 HashCode ，如果是延时消息，存储的是计划消费时间
                storeTimestamp, // 消息存储时间
                queueOffset, // 消息逻辑队列偏移量
                keys, // 提供一个或一组关键字，用于消息的索引或去重等功能。
                uniqKey, // msgId 于确保消息的全局唯一性，或是作为消息去重的依据。
                sysFlag, // 消息系统标记 包含消息处理的系统级别标志位，如是否是延迟消息、是否事务消息等信息。
                preparedTransactionOffset,  // 预处理事务偏移量，仅当消息属于事务消息时使用，表示事务消息预提交状态的偏移量
                propertiesMap // 消息的附加属性
            );
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    /**
     * 根据消息体、主题和属性的长度，结合消息存储格式，计算消息的总长度
     * @param sysFlag
     * @param bodyLength
     * @param topicLength
     * @param propertiesLength
     * @return
     */
    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storehostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;

        /**
         * RocketMQ 消息存储格式如下：
         */
        final int msgLen =
              4 //TOTALSIZE 消息条目总长度， 4字节，注意：CommitLog 条目是不定长的，每个条目的长度存储在前 4 个字节，包含消息头和消息体的总长度
            + 4 //MAGICCODE 魔数 用于识别和验证消息格式 4字节
            + 4 //BODYCRC 消息体的 crc 校验码 4 字节，用来检测消息体在存储或传输过程中是否损坏 4字节
            + 4 //QUEUEID 消息消费队列id 4字节
            + 4 //FLAG    消息标记，RocketMQ 对其不做处理，供应用程序使用 ，4字节
            + 8 //QUEUEOFFSET 消息在 ConsumerQueue 文件中的逻辑偏移量 8字节
            + 8 //PHYSICALOFFSET 消息在 CommitLog 文件中的物理偏移量 8字节
            + 4 //SYSFLAG 消息系统标记 例如是否压缩，是否是事务消息等 4字节
            + 8 //BORNTIMESTAMP 消息生产者调用消息发送API的时间戳，8字节
            + bornhostLength //BORNHOST 消息发送者的IP、端口号 8字节
            + 8 //STORETIMESTAMP 消息存储时间戳 8字节
            + storehostAddressLength //STOREHOSTADDRESS 服务器的IP、端口号 8字节
            + 4 //RECONSUMETIMES 消息重试次数 4字节
            + 8 //Prepared Transaction Offset 事务消息的物理偏移量 8字节
            + 4 + (bodyLength > 0 ? bodyLength : 0) //BODY 消息体长度 4 字节
            + 1 + topicLength //TOPIC Topic 主题存储长度 1字节，从这里也可以看出主题名称不能超过 255 个字符
            + 2 + (propertiesLength > 0 ? propertiesLength : 0) //propertiesLength 消息属性长度 2字节，从这里也可以看出，消息属性长度不能超过 65536
            + 0;
        return msgLen;
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        mappedFileOffset += size;

                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                            if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                                this.defaultMessageStore.doDispatch(dispatchRequest);
                            }
                        } else {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    else if (size == 0) {
                        index++;
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {
                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
        long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
        if (0 == storeTimestamp) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
            && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                    storeTimestamp,
                    UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    private void notifyMessageArriving() {

    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    /**
     * 异步存储 Message
     * @param msg
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 获取消息要发送的 Topic 和 queueId
        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        // 获取消息的系统标签
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        // todo 非事务消息才可进行延迟发送。MessageSysFlag.TRANSACTION_COMMIT_TYPE 是事务结束的标识
        // todo 注意：半消息在之前的流程中，已经将消息的系统标签设置为非事务
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
                || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            // 延迟级别
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                // 备份消息真实的 Topic 和 queueId 属性
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;

        // 获取 CommitLog 对应的映射文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 上锁，独占锁或自旋锁
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            // todo 加锁，记录开始锁定时间
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            // todo 记录消息被存储的时间戳
            msg.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }
            // todo 往 commitlog 文件中写入消息，追加消息
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                // 创建一个新文件继续写
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }
        // todo 热映射文件启用 && 前面写入消息时，用的是新生成的映射文件，
        // 锁定这块新生成的映射文件内存，防止该内存空间被 swap 出去
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        // 创建追加消息的结果
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        // todo 将该 Topic 主题下的消息总数+1
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        // todo 记录下该 Topic 主题下的此次消息内容大小
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        // todo 提交消息刷盘请求，可能同步或异步，根据配置
        CompletableFuture<PutMessageStatus> flushResultFuture = submitFlushRequest(result, msg);

        // todo 提交复制请求：可能同步或异步，根据发送消息时 Message 附加属性的 WAIT 的值
        // 该方式对同步复制进行了优化，putMessage 中的该方法 sendMessageThread 线程只有收到从节点的同步完成结果，才能继续处理后续消息
        // 而该方法减少了 sendMessageThread 线程的等待时间，即在同步复制的过程中，该线程可以继续处理后续请求，在收到从节点同步结果后，再向客户端返回结果，也就是异步感知同步完成了。
        CompletableFuture<PutMessageStatus> replicaResultFuture = submitReplicaRequest(result, msg);

        // 合并消息刷盘结果、消息同步其他节点的结果，并返回 PutMessageResult
        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            // 刷盘任务
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }

            // 复制任务
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
                if (replicaStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
                    log.error("do sync transfer other node, wait return, but failed, topic: {} tags: {} client address: {}",
                            msg.getTopic(), msg.getTags(), msg.getBornHostNameString());
                }
            }
            return putMessageResult;
        });
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        CompletableFuture<PutMessageStatus> flushOKFuture = submitFlushRequest(result, messageExtBatch);
        CompletableFuture<PutMessageStatus> replicaOKFuture = submitReplicaRequest(result, messageExtBatch);
        return flushOKFuture.thenCombine(replicaOKFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
                if (replicaStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
                    log.error("do sync transfer other node, wait return, but failed, topic: {} client address: {}",
                            messageExtBatch.getTopic(), messageExtBatch.getBornHostNameString());
                }
            }
            return putMessageResult;
        });

    }

    /**
     * 接收并存储消息
     * 说明：
     * 先将消息写入到 MappedFile 内存映射文件
     * 再根据刷盘策略写入到磁盘
     * <p>
     * CommitLog 写入消息过程简单描述：
     * 1. 获取消息类型
     * 2. 获取一个 MappedFile 对象，内存映射的具体实现
     * 3. 追加消息需要加锁，进行串行化操作
     * 4. 确保获取一个可用的 MappedFile (如果没有，则创建一个)
     * 5. 通过 MappedFile 对象写入文件
     * 6. 根据刷盘策略， 进行刷盘
     * 7. 主从同步，如果有的情况下
     * </p>
     * @param msg 消息
     * @return 存储消息的结果
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // Set the storage time
        // 设置存储时间
        msg.setStoreTimestamp(System.currentTimeMillis());
        // Set the message body BODY CRC (consider the most appropriate setting
        // on the client)
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // Back to Results
        AppendMessageResult result = null;

        // 获取存储统计服务
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 获取消息所属的 Topic 和 queueId
        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        // 1. 获取消息类型，如事务消息、非事务消息、 Commit 消息
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());

        // 2. 如果是非事务消息 或 事务提交消息
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE
            || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // Delay Delivery
            /**
             * todo 根据消息内的延迟级别 > 0 ，判断是否是延迟消息
             * 1. level 有以下三种情况：
             *  - level == 0 消息未非延迟消息
             *  - 1 <= level <= maxLevel 消息延迟特定时间，例如 level = 1，延迟 1s
             *  - level > maxLevel 则 level == maxLevel 例如 level = 20 延迟 2h
             * 2. 工作流程
             *  - 定时消息会暂存在名为 SCHEDULE_TOPIC_XXXX 的 Topic 下，并根据 delayTimeLevel 存入特定的 queue，queueId = (delayTimeLevel - 1) 即一个 Queue 只存在相同延迟的消息，保证具有相同发送延迟的消息能够顺序消费。
             *  - Broker 会调度地消费 SCHEDULE_TOPIC_XXXX ，将消息写入真实的 Topic
             *  - 需要注意的是：定时消息会在第一次写入和调度写入真实 Topic 时都会计数，因此发送数量， tps 会变高。
             */
            if (msg.getDelayTimeLevel() > 0) {
                // 超过最大的延迟级别，则使用最大的延迟级别
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }

                // 获取延迟消息专用的 Topic 和队列id
                topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // Backup real topic, queueId
                // 备份真实的 Topic 和 queueId ，将它们放到 Message 的 property 属性中
                // REAL_TOPIC
                // REAL_QUEUE_ID
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                // 将消息的 Topic 和 queueId 替换成延迟队列的 Topic 和 queueId
                // 这样就保证消息不会立即被发送出去，而是经过 SCHEDULE_TOPIC_XXXX 的特殊处理，然后再发送到 Consumer
                // 在没有经过 SCHEDULE_TOPIC_XXXX 特殊处理之前，与消费者实际对应的 Topic 没有关系
                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;

        // 3. 获取当前可以写入的 CommitLog 文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 4. todo 追加消息时，需要加锁，串行化处理
        // 使用自旋锁(while-CAS) 或 ReentrantLock
        putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
        try {
            // 写消息加锁的开始时间戳
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            // 设置消息存储时间
            msg.setStoreTimestamp(beginLockTimestamp);

            // 5. 检验 MappedFile 对象，获取一个可用的 MappedFile
            if (null == mappedFile || mappedFile.isFull()) {
                // 没有的话，会创建一个新的 MappedFile
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }

            // 文件创建失败，可能是磁盘空间不足或者没有权限导致的
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            // 6. 通过 MappedFile 对象写入文件，即存储消息
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);

            // 根据写入结果作出判断
            switch (result.getStatus()) {
                case PUT_OK:
                    break;

                // MappedFile 不够写了(填空占位)，创建新的重新写消息
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    // 追加消息
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;

                // 以下都是异常流
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            // 写消息耗时
            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;

            // 释放锁
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        // 创建写入消息的结果
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics 统计
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        /*******************上面追加消息只是将消息追加到内存中，需要根据采取的同步刷盘还是异步刷盘的方式，将内存中的数据，持久化到磁盘*****************************************************/

        // todo 重要
        // 7. 根据刷盘策略刷盘，即持久化到文件，前面的流程，实际并没有将消息持久化到磁盘
        handleDiskFlush(result, putMessageResult, msg);

        // 8. todo 执行 HA 主从复制，根据是否等待，决定是同步复制还是异步复制
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }

    /**
     * 提交刷盘请求
     * @param result
     * @param messageExt
     * @return
     */
    public CompletableFuture<PutMessageStatus> submitFlushRequest(AppendMessageResult result, MessageExt messageExt) {
        // Synchronization flush 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;

            // 是否等待消息存储成功
            if (messageExt.isWaitStoreMsgOK()) {
                // 创建刷盘请求对象
                // 入参：1.提交的刷盘点(就是预计刷到哪里)，result.getWroteOffset() + result.getWroteBytes()
                // 入参：2.刷盘超时时间
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
                        this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());

                // 加入到刷盘任务队列
                service.putRequest(request);

                // 返回刷盘结果(CompletableFuture)
                return request.future();

                // 无需等待存储消息结果
            } else {
                service.wakeup();
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }
        // Asynchronous flush 异步刷盘
        else {
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else  {
                commitLogService.wakeup();
            }
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }
    }

    /**
     * 提交复制请求 - 异步
     * @param result
     * @param messageExt
     * @return
     */
    public CompletableFuture<PutMessageStatus> submitReplicaRequest(AppendMessageResult result, MessageExt messageExt) {
        // 当前 Broker 是 Master 才会处理复制请求
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {

            // 获取 HA 服务，主从同步实现服务
            HAService service = this.defaultMessageStore.getHaService();

            // 是否等待存储后返回 - 是否等待复制完成
            // todo 等待复制完成
            if (messageExt.isWaitStoreMsgOK()) {
                if (service.isSlaveOK(result.getWroteBytes() + result.getWroteOffset())) {
                    // 创建同步刷盘请求对象
                    // todo 提交的复制点：result.getWroteOffset() + result.getWroteBytes()
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(),
                            this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());

                    // todo 向 HAService 提交同步刷盘请求
                    service.putRequest(request);

                    // todo 唤醒、执行刷盘
                    service.getWaitNotifyObject().wakeupAll();

                    // 注意：这里返回的不是同步结果，而是一个 completeFuture 该 complete 方法会在消息被复制到从节点后被调用的
                    return request.future();
                }
                else {
                    return CompletableFuture.completedFuture(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

        // 当前 Broker 非 Master
        return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
    }

    /**
     * 消息刷盘
     * 分为：同步刷盘和异步刷盘，默认为异步刷盘
     * @param result
     * @param putMessageResult
     * @param messageExt
     */
    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // Synchronization flush 同步刷盘（将刷盘任务提交到刷盘线程，等待其执行完成）
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {

            // todo 1.同步刷盘服务类
            // todo 里面包含刷盘执行逻辑，等待-通知机制 -- RocketMQ 自实现的可重复使用的 CountDownLatch
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;

            // 如果要等待存储结果
            if (messageExt.isWaitStoreMsgOK()) {

                // 1. 构建同步刷盘任务，刷盘规则：
                // 一个 GroupCommitRequest 请求就是一个刷盘任务
                // 在刷盘前，刷盘线程会判断已经刷盘的位置和 (result.getWroteOffset() + result.getWroteBytes()) 比较，只要还存在没有刷盘的数据，就刷盘
                // todo 脏页，其实是在内存中已经修改的数据，但是没有 Flush 到磁盘
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());

                // 2. 将刷盘任务提交同步刷盘服务类的【写任务队列】中，有刷盘线程周期性处理任务
                service.putRequest(request);

                // todo 每个 GroupCommitLogRequest 对象都包含一个 CompletableFuture 对象，用于异步通知刷盘结果
                // 3. 等待同步刷盘任务完成
                CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
                PutMessageStatus flushStatus = null;
                try {
                    /**
                     * 阻塞等待同步刷盘完成，因为刷盘任务已经提交给了刷盘线程，默认等待 5s
                     * @see GroupCommitService#doCommit() 中完成刷盘，会标记完成状态
                     */
                    flushStatus = flushOkFuture.get(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(),
                            TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    //flushOK=false;
                }

                // 如果刷盘不成功，则设置刷盘超时
                if (flushStatus != PutMessageStatus.PUT_OK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags()
                        + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
                // 不需要等待存储结果，直接唤醒同步刷盘，因为可能此时正处于等待状态
            } else {
                service.wakeup();
            }
        }

        // todo 二、异步刷盘
        // Asynchronous flush
        else {
            // 是否开启 内存级别的 读写分离机制
            // 没有开启
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                // 唤醒异步刷盘线程，因为可能此时正处于等待的状态，让它立即醒来处理刷盘
                flushCommitLogService.wakeup();

            // 开启
            } else {
                // 唤醒提交线程，因为可能此时正处于等待状态，让它立即醒来提交堆外内存到物理文件的内存映射
                // todo 注意：提交线程后，也会立即唤醒刷盘线程，可能是同步刷盘，也有可能是异步刷盘
                commitLogService.wakeup();
            }
        }
    }

    /**
     * 处理复制(主从同步的逻辑)
     * @param result
     * @param putMessageResult
     * @param messageExt
     */
    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // 当前 Broker 是 Master
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {

            // 获取主从同步核心实现对象
            HAService service = this.defaultMessageStore.getHaService();

            // 是否等待复制完成
            // 如果是就是同步复制
            if (messageExt.isWaitStoreMsgOK()) {
                // Determine whether to wait
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    // 创建同步复制请求，并将同步复制请求加入到任务队列中
                    // todo 会有同步复制任务判断复制是否完成，判断的依据是 result.getWroteOffset() + result.getWroteBytes()
                    // todo 注意：同步复制任务处理 GroupCommitRequest 请求并不是执行复制，而是判断复制是否完成。复制工作不是通过同步复制任务来完成的，而是通过主服务器和从服务器来完成的。
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    service.putRequest(request);

                    // 唤醒任务，去判断复制是否完成
                    service.getWaitNotifyObject().wakeupAll();
                    PutMessageStatus replicaStatus = null;
                    try {
                        // todo 同步等待复制结果，直到有结果返回或超时
                        // 注意：这里等待复制结果超时时间和同步刷盘超时时间一致，默认都是 5s
                        replicaStatus = request.future().get(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(),
                                TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    }

                    // 如果同步失败，则返回失败
                    if (replicaStatus != PutMessageStatus.PUT_OK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: "
                            + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                }
                // 从服务器异常
                // Slave problem
                else {
                    // Tell the producer, slave not available
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

    }

    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        //fine-grained lock instead of the coarse-grained
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();

        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        putMessageLock.lock();
        try {
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly
            // global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }

            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    unlockMappedFile = mappedFile;
                    // Create a new file, re-write the message
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        handleDiskFlush(result, putMessageResult, messageExtBatch);

        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    /**
     * 获取当前 CommitLog 的最小偏移量 (非某个 CommitLog 文件)
     * @return
     */
    public long getMinOffset() {
        // 获取 CommitLog 目录下的第一个文件
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();

        // 如果第一个文件是可用的，返回文件的起始偏移量，否则返回下一个文件的起始偏移量
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * todo 读取从物理偏移量到 size 大小的数据，如：size = 4 读取的就是消息的长度(因为 CommitLog 和 ConsumerQueue 存储不一样，前者是不定长的，后者是定长的 20字节)
     * 说明：
     * 主要根据物理偏移量，找到所在的 CommitLog 文件，CommitLog文件 封装成 MappedFile(内存映射文件)，然后直接从偏移量开始，读取指定的字节(消息的长度)
     * 要是事先不知道消息的长度，只知道 Offset呢？先找到 MappedFile，然后从 Offset 处先读取 4 个字节，就能获取该消息的总长度
     * @param offset 物理偏移量
     * @param size 消息大小
     * @return
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        // 1. 获取 CommitLog 文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 2. 根据偏移量 Offset ，找到偏移量所在的 CommitLog 文件
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        // 3. 找到对应的 MappedFile 后，开始根据偏移量和消息长度查找消息
        if (mappedFile != null) {
            // 根据 Offset 计算在某个 MappedFile 中的偏移量
            int pos = (int) (offset % mappedFileSize);
            // 从偏移量读取 size 长度的内容并返回
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    /**
     * 根据 Offset ，返回 Offset 所在 CommitLog 文件的下一个 CommitLog 文件的起始偏移量
     * @param offset 物理偏移量
     * @return
     */
    public long rollNextFile(final long offset) {
        // 获取每个 CommitLog 文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // (offset + 文件大小) --> 跳到下一个文件
        // 减去多余的 Offset ，就可以得到起始偏移量
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    /**
     * 刷盘任务
     * 说明：MappedFile 落盘方式
     * 1. 开启使用堆外内存，数据先追加到堆外内存 --> 提交堆外内存数据到与物理文件的内存映射中 --> 物理文件的内存映射 Flush 到磁盘
     * 2. 没有开启使用堆外内存，数据直接追加到与物理文件的内存映射中 --> 物理文件的内存映射 Flush 到磁盘
     * 同步刷盘的具体实现：GroupCommitService
     * 异步刷盘的具体实现：FlushRealTimeService
     * 从堆外内存提交到物理文件的内存映射中 具体实现：CommitRealTimeService
     */
    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /*****************************提交堆外内存到内存映射******************************/

    /**
     * 提交到物理文件的内存映射中，相当于：
     * 异步刷盘 && 开启内存字节缓冲区
     * 说明：消息插入成功时，刷盘时使用，和 FlushRealTimeService 过程类似，提交的消息会被刷盘线程定时刷盘
     */
    class CommitRealTimeService extends FlushCommitLogService {

        // 最后 Commit 的时间戳
        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        /**
         * 默认每 200ms 将 Bytebuffer 新追加的数据提交到 FileChannel 中
         */
        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            // Broker 不关闭，线程会不断轮询
            while (!this.isStopped()) {

                // 任务执行间隔时间 默认 200ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                // 一次提交任务至少包含的页数，如果待提交数据不足，小于该参数配置的值，将忽略本次提交的任务， 默认 4 页
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                // 两次真实提交的最大时间间隔，默认 200ms
                int commitDataThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                // 如果距上次提交时间超过 commitDataThoroughInterval，则强制提交，默认 200ms，
                // 所谓强制提交，就是会忽略 commitDataLeastPages 参数，也就是即使 待提交数据不足 commitDataLeastPages，也会执行提交操作
                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    // 提交 Bytebuffer 到 FileChannel 即将待提交数据，提交到物理文件的内存映射中
                    // todo 注意：如果返回 false 并不是代表提交失败，而是表示有数据提交成功了
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();

                    // todo 有数据提交成功，立即唤醒刷盘线程执行刷盘操作，没有等待忽略本次通知即可
                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        //now wake up flush thread.
                        // 如果刷盘线程现在在等待，则立即唤醒它，进行刷盘
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }

                    // todo 每执行完成一次提交操作，将默认等待 200ms ，再继续执行提交操作
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Broker 关闭，强制提交所有待提交的数据
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /***********************************异步刷盘**************************/

    /**
     * 异步刷盘 && 关闭内存字节缓冲区
     * 说明：实时 Flush 线程服务，调用 MappedFile#flush 相关逻辑
     */
    class FlushRealTimeService extends FlushCommitLogService {
        // 最后 Flush 的时间戳
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        /**
         * 休眠等待 或 超时等待唤醒进行刷盘
         */
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            // Broker 没有关闭，线程没有停止
            while (!this.isStopped()) {

                // 每次循环是固定周期还是等待唤醒，默认等待唤醒
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                // 刷盘任务执行的时间间隔，默认 500ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();

                // 一次刷盘任务至少包含页数，如果待刷盘数据不足，小于该参数配置的值，将忽略本次刷盘任务，默认 4 页
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                // 两次真实刷盘的最大间隔时间，默认 10s
                int flushPhysicQueueThoroughInterval =
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress

                // 如果当前时间 >= 最后一次刷盘时间 + flushPhysicQueueThoroughInterval，那么本次刷盘，将会忽略 flushPhysicQueueLeastPages 参数，也就是即使写入的数据量不足 flushPhysicQueueLeastPages 参数配置的值，也会刷盘所有数据
                // 即： 每隔 FlushPhysicQueueThoroughInterval 周期执行一次 Flush， 但不是每次循环都能满足 FlushCommitLogLeastPages 大小
                // 因此，需要一定周期进行一次强制的 Flush，当然，不是每次循环都去执行强制 Flush ，这样性能较差
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    // 每隔 10 次打印一次刷新进度
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    // 根据 FlushCommitLogTimed 参数，可以选择每次循环是固定周期或者等待唤醒
                    // 默认配置是等待唤醒，所以，每次写入消息完成，会去调用 CommitLogService.wakeup()
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        // 等待多长时间被唤醒
                        this.waitForRunning(interval);
                    }

                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    // todo 调用 MappedFile 进行 Flush
                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);

                    // 更新 checkPoint 文件中的 CommitLog 的更新时间
                    // checkpoint 文件的刷盘动作在 ConsumeQueue 线程中执行
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            // 执行到这里，说明 Broker 关闭，强制 Flush ，避免有未刷盘的数据，重试 10次
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    public static class GroupCommitRequest {
        private final long nextOffset;
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private final long startTimestamp = System.currentTimeMillis();
        private long timeoutMillis = Long.MAX_VALUE;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.timeoutMillis = timeoutMillis;
        }

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }


        public long getNextOffset() {
            return nextOffset;
        }

        public void wakeupCustomer(final PutMessageStatus putMessageStatus) {
            this.flushOKFuture.complete(putMessageStatus);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }

    }

    /**
     * GroupCommit Service
     * 消息追加成功后，同步刷盘时使用
     */
    class GroupCommitService extends FlushCommitLogService {
        // 写队列，用于存放刷盘任务
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();
        // 读队列，用于线程读取任务
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        /**
         * 添加刷盘任务到写队列，用于线程读取任务
         * 说明：此处使用 synchronized 关键字是为了保证添加刷盘任务能够稳定进行，
         * 因为 this.requestsWrite 和 this.requestsRead 会频繁的交换
         * @param request
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }

            /*
            如果有刷盘任务，就创造立即刷盘条件，即唤醒可能阻塞等待的 GroupCommitService
            public void wakeup() {
                if (hasNotified.compareAndSet(false, true)) {
                    waitPoint.countDown(); // notify
                }
            }
            */
            this.wakeup();
        }

        /**
         * 切换读写队列
         * todo 亮点设计：避免任务提交和任务执行的锁冲突，每次同步刷盘线程进行刷盘前都会将写队列切换到读队列，这样写队列可以继续接收刷盘请求，而刷盘线程直接从读队列读取任务进行刷盘
         */
        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 刷盘
         */
        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    // 遍历刷盘请求
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        // todo 是否需要刷盘：对比 flush 的 offerSet  和 请求的 Offset，看是否需要刷盘
                        boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        // todo 为什么刷盘循环两次？有可能每次循环写入的消息，可能分布在两个 MapperFile 文件里面(写 N 个消息时，MapperFile文件满了，需要写入新的 MapperFile 文件)，所以需要循环两次
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            // 需要刷盘时，这里将刷盘页设置为 0 ，表示立即刷盘
                            CommitLog.this.mappedFileQueue.flush(0);
                            // 判断是否刷盘成功，通过比较 Flush 的 offerSet 和 req 的 offerSet
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        }
                        // todo 通知等待同步刷盘的线程刷盘完成，避免一直等待，即标记 FlushOKFuture 完成
                        req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    // todo 处理完成刷盘任务之后，更新 checkpoint 中的 CommitLog 刷盘时间
                    // todo 疑惑：此时并没有执行检测点的刷盘操作，刷盘检测点的刷盘操作将在刷写消息队列文件时触发
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    // 清理读队列
                    // 每次刷盘时，会把写队列切换为读队列
                    this.requestsRead.clear();
                } else {
                    // Because of individual messages is set to not sync flush, it
                    // will come to this process
                    // 直接刷盘，此处是由于发送的消息，isWaitStoreMsgOk 为 false，导致刷盘时，没有刷到磁盘，此时需要刷盘
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        /**
         * 线程一直处理同步刷盘，每处理一个循环后，等待 10ms 后。一旦新任务到达，立即唤醒执行任务
         */
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 等待 10ms
                    this.waitForRunning(10);
                    // 执行刷盘操作
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * 追加消息回调
     */
    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        /**
         * 存储在内存中的消息编号字节Buffer
         */
        private final ByteBuffer msgIdMemory;
        private final ByteBuffer msgIdV6Memory;
        // Store the message content
        /**
         * 存储在内存中的消息内容字节Buffer
         */
        private final ByteBuffer msgStoreItemMemory;
        // The maximum length of the message
        /**
         * 消息的最大长度
         */
        private final int maxMessageSize;
        // Build Message Key
        /**
         * {@link #topicQueueTable} 的 key
         * 构建消息 Key
         * 计算方式：Topic + "-" + queueId
         */
        private final StringBuilder keyBuilder = new StringBuilder();

        private final StringBuilder msgIdBuilder = new StringBuilder();

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        /**
         * 消息写入字节缓冲区
         *
         * @param fileFromOffset 该文件在整个文件组中的偏移量，文件起始偏移量
         * @param byteBuffer NIO 字节容器
         * @param maxBlank 最大可写字节，文件大小-写入位置
         * @param msgInner 消息内部封装实体
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBrokerInner msgInner) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>

            // PHY OFFSET
            // 1. todo 物理偏移量，即相对整个 CommitLog 文件组，消息已经写到哪里了，目前写到了 wroteOffset
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // 消息类型
            int sysflag = msgInner.getSysFlag();

            int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);
            this.resetByteBuffer(storeHostHolder, storeHostLength);

            // todo 这里应该叫 OffsetMsgId，该 id 包含很多信息
            String msgId;

            // 2. 格式：4字节当前 Broker IP + 4字节当前 Broker 端口号 + 8字节消息物理偏移量
            // todo 在 RocketMQ 中，只需要提供 OffsetMsgId 可不必知道该消息所属的 Topic 信息即可查询该条消息的内容
            if ((sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(storeHostHolder), wroteOffset);
            } else {
                msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, msgInner.getStoreHostBytes(storeHostHolder), wroteOffset);
            }

            // Record ConsumeQueue information
            // 3. todo 根据 Topic-queueId 获取该队列的偏移地址(待写入地址)，如果没有，新增一个键值对，当前偏移量为 0
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // Transaction messages that require special handling
            // 4. todo 对事务消息需要单独特殊的处理(PREPARED、ROLLBACK 类型的消息，不能进入 Consumer 队列)
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the
                // consumer queuec
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /**
             * Serialize message
             */
            // 5. todo 对消息进行序列化，并获取序列化后的消息长度
            // 5.1 消息的附加属性长度不能超过 65536 个字节
            final byte[] propertiesData =
                msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);

            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;

            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            // 5.2 获取 Topic 长度
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            // 5.3 获取消息体长度
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            // 5.4 获取消息长度
            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            // Exceeds the maximum message
            // 5.5 如果消息长度超过配置值，则返回错误:MESSAGE_SIZE_EXCEEDED
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength
                    + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // Determines whether there is sufficient free space
            // 6 todo 如果 MappedFile 的剩余空间不足，写入 BLANK 占位，则返回错误:END_OF_FILE，后续会新创建 CommitLog 文件来存储该消息
            // todo 从这里看出，每个 CommitLog 文件最少空闲 8 个字节，高 4 字节存储当前文件的剩余空间，低 4 字节存储魔数 CommitLog.BLANK_MAGIC_CODE
            // 也就是文件可用的空间放不下一个消息，为了区分，在每一个 CommitLog 文件的最后会写入 8 个字符，表示文件的结束
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                // 1 TOTALSIZE 消息总长度 4 字节
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE 魔数 4 个字节
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);

                // 返回 END_OF_FILE
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(),
                    queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            /**********************7. 将消息根据消息的结构，顺序写入 MappedFile 文件中(内存)************************************/
            // Initialization of storage space
            // todo 重置 msgStoreItemMemory 指定只能写 msgLen 长度
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE 消息总长度 4个字节，注意： CommitLog 条目是不定长的，每个条目的长度存储在 4 个字节
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE 魔数 4个字节
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC 消息体的 crc 校验码 4个字节
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID 消息消费队列id 4个字节
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG 消息标记，RocketMQ 对其不做处理，供应用程序使用，默认 4个字节
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET 消息队列逻辑偏移量 8个字节
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET 消息在 CommitLog 文件中的物理偏移量 8个字节
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG 消息系统标记，例如是否压缩，是否是事务消息等 4个字节
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP 消息生产者调用消息发送 API 的时间戳 8个字节
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST 消息发送者的 IP 地址和端口号 8个字节
            this.resetByteBuffer(bornHostHolder, bornHostLength);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP 消息存储的时间戳 8个字节
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS 消息存储的 Broker 的 IP 地址和端口号 8个字节
            this.resetByteBuffer(storeHostHolder, storeHostLength);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
            // 13 RECONSUMETIMES 消息重试次数
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY 消息体长度和具体的消息内容 4个字节
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC Topic主题存储长度和内容 1字节
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES 消息属性长度 2 字节
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0)
                this.msgStoreItemMemory.put(propertiesData);

            // 将消息存储到 ByteBuffer
            // todo 注意：这里只是将消息存储在 MappedFile 对应的内存映射 Buffer 中，并没有写入磁盘
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // Write messages to the queue buffer
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);

            // 8. 返回消息写入结果，其状态为 PUT_OK 即写入 MappedFile 成功
            AppendMessageResult result = new AppendMessageResult(
                    AppendMessageStatus.PUT_OK,
                    wroteOffset,
                    msgLen,
                    msgId, // offsetMsgId
                    msgInner.getStoreTimestamp(),
                    queueOffset,
                    CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    // todo 更新消息队列的逻辑偏移量，类型下标++
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }
            return result;
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
            final MessageExtBatch messageExtBatch) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;
            msgIdBuilder.setLength(0);
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            this.resetByteBuffer(storeHostHolder, storeHostLength);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(storeHostHolder);
            messagesByteBuff.mark();
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();
                final int bodyLen = msgLen - 40; //only for log, just estimate it
                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(),
                        beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                storeHostBytes.rewind();
                String msgId;
                if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                    msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                } else {
                    msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                }

                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(),
                messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    public static class MessageExtBatchEncoder {
        // Store the message content
        private final ByteBuffer msgBatchMemory;
        // The maximum length of the message
        private final int maxMessageSize;

        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            msgBatchMemory.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            // properties from MessageExtBatch
            String batchPropStr = MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
            final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
            final short batchPropLen = (short) batchPropData.length;

            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength,
                        propertiesLen + batchPropLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen
                        + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET
                this.msgBatchMemory.putLong(0);
                // 7 PHYSICALOFFSET
                this.msgBatchMemory.putLong(0);
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(bornHostHolder, bornHostLength);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(bornHostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(storeHostHolder, storeHostLength);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0)
                    this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort((short) (propertiesLen + batchPropLen));
                if (propertiesLen > 0) {
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
                }
                if (batchPropLen > 0) {
                    this.msgBatchMemory.put(batchPropData, 0, batchPropLen);
                }
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        /**
         * 重置字节缓存区
         * @param byteBuffer
         * @param limit
         */
        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}
