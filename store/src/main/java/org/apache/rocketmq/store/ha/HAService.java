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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageStatus;

/**
 * RocketMQ 主从同步核心类
 * todo 注意：该类 Master 和 slave 都会使用，具体根据不同的角色发挥不同的作用。
 * <p>
 *  RocketMQ HA 机制大致可以分为以下三个部分
 *  1. Master 启动并监听 slave 的连接
 *  2. slave 启动，主动与 Master 建立连接，Master 接收 slave 的连接，并建立相关 TCP 连接
 *  3. slave 以每隔 5s 的间隔时间，上报待拉取偏移量，等待 Master 返回数据
 *  4. slave 保存消息并继续上报新的消息同步请求
 *  todo 主从复制的关键两大步骤
 *  1. 复制工作是由 HAConnection 和 HAClient 完成的，具体是
 *  - 1.1 HAService 启动，根据当前服务器的角色，可能是 Master ，也可能是 slave
 *  - 1.2 Master 会打开通道，监听 slave 的连接，一旦有 slave 连接就会封装建立的通道，并启动两个线程任务，分别用于读取从服务器复制消息请求，向从服务器发送复制的消息
 *  - 1.3 slave 会启动线程任务，完成连接 Master，向 Master 上报自己的复制偏移量(向 Master 发送复制请求)，处理 Master 回传的消息，并追加到 CommitLog 文件中
 *  以上三个过程不停执行，完成 slave 和 Master 的交互，实现主从复制
 *  <p>
 *  2. 消息发送者执行主从复制
 *  - 2.1 这个和真正的主从复制没有关系，严格来说这个是判断主从复制是否完成的逻辑，因为主从复制是另一个逻辑完成的。
 *  - 2.2 GroupTransferService 用于实现消息生产方等待主从复制完成或超时
 *  - 2.3 主从复制是不依赖外部应用程序的，是 RocketMQ 内部逻辑实现的
 *  - 2.3.1 从服务器使用 HAClient 线程任务和主服务器通信，不断向主服务器上报同步进度以及读取主服务器发送的消息并写入到本地
 *  - 2.3.2 主服务器使用 AcceptSocketService 线程任务接收从服务的连接请求，并创建 HAConnection 封装 Socket，然后通过 HAConnection 对象中的读写线程任务分别完成
 *  - 2.3.2.1 不断读取从服务器上报的消息复制进度，主要用于主服务器首次同步消息确定发送消息的位置，以及根据从服务器上报的复制进度尝试唤醒等待的线程
 *  - 2.3.2.2 主服务器根据自己维护的偏移量，不断拉取消息并发送给从服务器
 * </p>
 */
public class HAService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * Master 维护的连接个数(slave 个数)
     */
    private final AtomicInteger connectionCount = new AtomicInteger(0);

    /**
     * 具体连接信息，是对主服务器的连接 SocketChannel 的封装
     */
    private final List<HAConnection> connectionList = new LinkedList<>();

    /**
     * 实现主服务器监听从服务器的连接 -- 线程任务
     */
    private final AcceptSocketService acceptSocketService;

    /**
     * Broker 存储实现
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 等待-通知对象
     * 利用 synchronized-wait-notify/notifyAll 实现
     */
    private final WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

    /**
     * 该 Master 所有 slave 中同步最大的偏移量
     * todo 非常重要，该属性会周期性的根据主从复制的情况，进行更新，是作为判断是否复制成功的参考量
     */
    private final AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    /**
     * 同步的主从复制线程任务 - 查询复制请求是否完成
     */
    private final GroupTransferService groupTransferService;

    /**
     * HA客户端实现，slave端网络的实现类
     */
    private final HAClient haClient;

    /**
     * 创建 HAService
     * 主要包括 3 大线程任务，HA 启动时才会启动这 3 大线程任务
     * @param defaultMessageStore
     * @throws IOException
     */
    public HAService(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;

        // 主服务器监听从服务器连接的线程任务
        // todo 主服务器使用
        this.acceptSocketService =
            new AcceptSocketService(defaultMessageStore.getMessageStoreConfig().getHaListenPort());

        // 主从复制的线程任务 -- 查询复制请求是否完成
        this.groupTransferService = new GroupTransferService();

        // HA 客户端实现，slave 端网络的实现类
        // todo 从服务器使用
        this.haClient = new HAClient();
    }

    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
            result
                && ((masterPutWhere - this.push2SlaveMaxOffset.get()) < this.defaultMessageStore
                .getMessageStoreConfig().getHaSlaveFallbehindMax());
        return result;
    }

    /**
     * 通知主从复制
     * 该方法是在主服务器收到从服务器的拉取请求后被调用
     * todo 本质上是通过更新 push2SlaveMaxOffset 的值来达到唤醒阻塞等待复制完成的消息发送者线程。因为判断是否复制完成的依据是 ，预期复制点 > push2SlaveMaxOffset
     * @param offset slave 下一次待拉取的消息偏移量，也可以认为是 slave 的拉取偏移量确认信息
     */
    public void notifyTransferSome(final long offset) {
        // 如果 slave 下一次待拉取的消息偏移量大于 push2SlaveMaxOffset ，则更新 push2SlaveMaxOffset
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            // 更新成功， 唤醒 GroupTransferService 任务线程
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            // 更新失败，重试
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    // public void notifyTransferSome() {
    // this.groupTransferService.notifyTransferSome();
    // }

    /**
     * HAService 启动流程
     * todo 注意：不管是 Master 还是 slave 都将按照上述流程启动，在内部的实现会根据 Broker 配置来决定真正开启的流程
     * @throws Exception
     */
    public void start() throws Exception {
        // 1. 开启服务端监听，用于处理 slave 客户端的连接请求，并启动对应的监听线程，处理监听逻辑
        // todo 针对 Master 角色
        this.acceptSocketService.beginAccept();
        this.acceptSocketService.start();

        // 2. 同步的主从复制线程任务 -- 查询复制请求是否完成
        this.groupTransferService.start();

        // 3. 启动 HA 客户端线程
        // todo 针对 slave 角色
        this.haClient.start();
    }

    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    public void shutdown() {
        this.haClient.shutdown();
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    /**
     * Master 端监听 slave 连接的实现类
     * 说明：将主服务器和从服务器的连接封装为 HAConnection 并加入到 HAService 中维护的缓存里
     * Listens to slave connections to create {@link HAConnection}.
     */
    class AcceptSocketService extends ServiceThread {

        /**
         * Broker 服务监听套接字(本地IP + 端口号)地址
         */
        private final SocketAddress socketAddressListen;

        /**
         * 服务端 Socket 通道，基于 NIO
         */
        private ServerSocketChannel serverSocketChannel;

        /**
         * 事件选择器 基于 NIO
         */
        private Selector selector;

        public AcceptSocketService(final int port) {
            this.socketAddressListen = new InetSocketAddress(port);
        }

        /**
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // 1. 打开服务端 Socket 通道
            this.serverSocketChannel = ServerSocketChannel.open();
            // 创建 selector
            this.selector = RemotingUtil.openSelector();
            this.serverSocketChannel.socket().setReuseAddress(true);

            // 2. 绑定监听端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            // 设置为非阻塞模式
            this.serverSocketChannel.configureBlocking(false);

            // 3. 注册连接事件
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                this.serverSocketChannel.close();
                this.selector.close();
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         * 该方法是标准的基于 NIO 的服务端程序实例
         * 1. 应用程序不断轮询 selector ，检查是否有准备好的事件
         * 2. 选择器每 1s 处理一次连接事件
         * 3. 连接事件就绪后，调用 ServerSocketChannel 的 accept() 方法创建 SocketChannel
         * 4. 为每个连接创建一个 HAConnection 对象，该 HAConnection 将负责主从数据同步逻辑
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 1. 每 1s 处理一次连接就绪事件
                    this.selector.select(1000);
                    // 检查是否有准备好的事件
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    // 如果有的话，就处理
                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            // 连接事件
                            if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {

                                // 2. 连接事件就绪后，创建 SocketChannel
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    HAService.log.info("HAService receive new connection, "
                                        + sc.socket().getRemoteSocketAddress());

                                    try {
                                        // 3. todo 创建与服务端数据传输的通道，该对象负责主从数据同步逻辑，是对连接通道的封装
                                        HAConnection conn = new HAConnection(HAService.this, sc);
                                        // todo 创建后，立即启动 HAConnection 的两大任务
                                        // 启动向从服务器写入数据的线程
                                        // 启动从从服务器读取数据的线程
                                        // 这两个线程不断轮询
                                        conn.start();

                                        // 4. 在 HAServer 中维护 HAConnection
                                        HAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String getServiceName() {
            return AcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * todo 同步复制任务，即 GroupTransferService 的职责就是判断主从同步是否结束，而非主从同步逻辑
     * 说明：
     * 1. 主从同步阻塞实现，消息发送者将消息刷写到磁盘后，需要继续等待新数据被传输到从服务器
     * 2. todo 从服务器数据的复制是 HAConnection 中的逻辑，具体通过开启两个线程实现的，所以消息发送者在这里需要等待数据传输的结果，即这里去判断 HAConnection 中的结果
     * 3. todo 该类的整体结构与同步刷盘实现类类似。
     *
     * @see CommitLog#handleHA(org.apache.rocketmq.store.AppendMessageResult, org.apache.rocketmq.store.PutMessageResult, org.apache.rocketmq.common.message.MessageExt)
     *
     * GroupTransferService Service
     */
    class GroupTransferService extends ServiceThread {

        private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
        /**
         * 写同步复制队列
         */
        private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new ArrayList<>();

        /**
         * 读同步复制队列
         */
        private volatile List<CommitLog.GroupCommitRequest> requestsRead = new ArrayList<>();

        /**
         * 接收同步复制请求
         * @param request
         */
        public synchronized void putRequest(final CommitLog.GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            // 唤醒线程,立即执行复制操作
            this.wakeup();
        }

        /**
         * 通知某个阻塞在 notifyTransferObject 上的线程
         */
        public void notifyTransferSome() {
            this.notifyTransferObject.wakeup();
        }

        /**
         * 交换读写复制队列
         */
        private void swapRequests() {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 消息发送者线程提交任务后，将被阻塞直到超时或 GroupTransferService 通知唤醒，注意，在同步复制的情况下，todo 也就是 GroupTransferService 的职责就是判断主从同步是否结束。
         * <p>
         * 1. todo 对 RequestRead 加锁，顺序处理消息发送者线程提交的 同步主从复制是否完成的查询请求。异步主从复制不会提交该请求。
         * 2. GroupTransferService 负责在主从同步复制结束后，通知由于等待同步结果而阻塞的消息发送者线程。
         * 3. todo 判断主从同步是否完成的依据，从服务器中已成功复制的消息最大偏移量是否大于、等于消息生产者发送消息后，消息服务端返回下一条消息的起始偏移量(也就是预期的复制点)
         * 3.1 如果是，则表示主从同步复制已经完成，唤醒消息发送线程
         * 3.2 如果否，等待 1s 再次判断，每个请求会循环判断 5 次
         * 4. 消息发送者返回有两种情况：
         * 4.1 等待超过 5s
         * 4.2 GroupTransferService 通知主从复制完成
         * </p>
         */
        private void doWaitTransfer() {
            // 对读同步复制队列加锁，顺序处理消息发送者线程提交的主从复制是否成功的查询请求
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    // 遍历读同步队列，其实是【查询复制是否完成的请求】
                    for (CommitLog.GroupCommitRequest req : this.requestsRead) {

                        // todo 判断是否完成主从同步
                        // 从服务器中已成功复制的消息最大偏移量是否大于、等于消息生产者发送消息后，消息服务端返回下一条消息的起始偏移量，也就预计复制点
                        boolean transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();

                        // 同步复制的超时时间，默认是 5s ，作为每个任务最多循环判断次数
                        long waitUntilWhen = HAService.this.defaultMessageStore.getSystemClock().now()
                            + HAService.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout();

                        // 循环检测是否完成复制
                        while (!transferOK && HAService.this.defaultMessageStore.getSystemClock().now() < waitUntilWhen) {
                            // 每检测一次休眠 1s
                            this.notifyTransferObject.waitForRunning(1000);

                            // todo 再次判断是否已经成功复制
                            transferOK = HAService.this.push2SlaveMaxOffset.get() >= req.getNextOffset();
                        }

                        if (!transferOK) {
                            log.warn("transfer messsage to slave timeout, " + req.getNextOffset());
                        }

                        // todo 根据复制结果，尝试唤醒等待的线程，成功或超时
                        req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }

                    // 每次处理完同步请求后，清理读队列，用于存储下次待处理同步复制请求
                    // todo 本质上是查询同步复制是否完成的请求，真正的同步复制请求不在这里处理
                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 等待 10ms 或被唤醒
                    this.waitForRunning(10);

                    // 查询复制是否完成
                    this.doWaitTransfer();
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupTransferService.class.getSimpleName();
        }
    }

    /**
     * HA 客户端实现 - 读取主服务器发回的消息 & 请求拉取消息 或 ACK 消息
     * todo 从服务器端的核心实现类，即专门用于从服务器
     */
    class HAClient extends ServiceThread {

        /**
         * Socket 读缓存区大小 4M
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;

        /**
         * 主服务器地址
         */
        private final AtomicReference<String> masterAddress = new AtomicReference<>();

        /**
         * 从服务器向主服务器发起主从同步的拉取偏移量 固定 8 个字节
         */
        private final ByteBuffer reportOffset = ByteBuffer.allocate(8);

        /**
         * 网络传输通道
         */
        private SocketChannel socketChannel;

        /**
         * NIO 事件选择器
         */
        private Selector selector;

        /**
         * 上一次写入消息的时间戳，todo 向主服务器上报(拉取) 消息后更新该属性
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        /**
         * 反馈到主服务器的，当前从服务器的复制进度，即 CommitLog 的最大偏移量(写入即可，不要求刷盘)
         * <p>
         * 初始化为从服务器的 CommitLog 的最大物理偏移量
         * </p>
         */
        private long currentReportedOffset = 0;

        /**
         * 本次已处理读缓存区的指针，即 ByteBufferRead 中已转发的指针
         */
        private int dispatchPosition = 0;

        /**
         * 读缓存区，大小为 4M
         */
        private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        /**
         * 读缓存区备份，与 bytebufferRead 进行替换
         * todo 主要用于处理 ByteBufferRead 中未包含一条完整的消息的处理逻辑，然后等待消息完整
         */
        private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        public HAClient() throws IOException {
            this.selector = RemotingUtil.openSelector();
        }

        public void updateMasterAddress(final String newAddr) {
            String currentAddr = this.masterAddress.get();
            if (currentAddr == null || !currentAddr.equals(newAddr)) {
                this.masterAddress.set(newAddr);
                log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
            }
        }

        /**
         * 是否向主服务器反馈已拉取消息偏移量
         * todo 依据，如果超过 5s 的间隔时间没有请求拉取则反馈
         * @return
         */
        private boolean isTimeToReportOffset() {
            // 距离上次写入消息的时间戳
            long interval =
                HAService.this.defaultMessageStore.getSystemClock().now() - this.lastWriteTimestamp;

            // 主服务器与从服务器的高可用心跳发送时间间隔默认为 5s
            boolean needHeart = interval > HAService.this.defaultMessageStore.getMessageStoreConfig()
                .getHaSendHeartbeatInterval();

            return needHeart;
        }

        /**
         * 向主服务器反馈当前拉取偏移量，这里有两重含义
         * 1. 对于从服务器来说，是向主服务器发送下次待拉取消息的偏移量
         * 2. 对于主服务器来说，既可以认为是从服务器本次请求拉取的消息偏移量，也可以理解为从服务器的消息同步 ACK 确认消息
         * todo 结果，向主服务器发送一个 8 字节的请求，请求包中包含的数据为从服务器消息文件的最大偏移量。
         *
         * @param maxOffset 从服务器当前的复制进度，一般都是消息的最大偏移量
         * @return
         */
        private boolean reportSlaveMaxOffset(final long maxOffset) {
            this.reportOffset.position(0);
            this.reportOffset.limit(8);
            this.reportOffset.putLong(maxOffset);
            this.reportOffset.position(0);
            this.reportOffset.limit(8);

            for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
                try {
                    this.socketChannel.write(this.reportOffset);
                } catch (IOException e) {
                    log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                    return false;
                }
            }

            lastWriteTimestamp = HAService.this.defaultMessageStore.getSystemClock().now();
            return !this.reportOffset.hasRemaining();
        }

        private void reallocateByteBuffer() {
            int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
            if (remain > 0) {
                this.byteBufferRead.position(this.dispatchPosition);

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
                this.byteBufferBackup.put(this.byteBufferRead);
            }

            this.swapByteBuffer();

            this.byteBufferRead.position(remain);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            this.dispatchPosition = 0;
        }

        private void swapByteBuffer() {
            ByteBuffer tmp = this.byteBufferRead;
            this.byteBufferRead = this.byteBufferBackup;
            this.byteBufferBackup = tmp;
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        boolean result = this.dispatchReadRequest();
                        if (!result) {
                            log.error("HAClient, dispatchReadRequest error");
                            return false;
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.info("HAClient, processReadEvent read socket < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.info("HAClient, processReadEvent read socket exception", e);
                    return false;
                }
            }

            return true;
        }

        private boolean dispatchReadRequest() {
            final int msgHeaderSize = 8 + 4; // phyoffset + size
            int readSocketPos = this.byteBufferRead.position();

            while (true) {
                int diff = this.byteBufferRead.position() - this.dispatchPosition;
                if (diff >= msgHeaderSize) {
                    long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                    int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                    long slavePhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                    if (slavePhyOffset != 0) {
                        if (slavePhyOffset != masterPhyOffset) {
                            log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                            return false;
                        }
                    }

                    if (diff >= (msgHeaderSize + bodySize)) {
                        byte[] bodyData = new byte[bodySize];
                        this.byteBufferRead.position(this.dispatchPosition + msgHeaderSize);
                        this.byteBufferRead.get(bodyData);

                        HAService.this.defaultMessageStore.appendToCommitLog(masterPhyOffset, bodyData);

                        this.byteBufferRead.position(readSocketPos);
                        this.dispatchPosition += msgHeaderSize + bodySize;

                        if (!reportSlaveMaxOffsetPlus()) {
                            return false;
                        }

                        continue;
                    }
                }

                if (!this.byteBufferRead.hasRemaining()) {
                    this.reallocateByteBuffer();
                }

                break;
            }

            return true;
        }

        private boolean reportSlaveMaxOffsetPlus() {
            boolean result = true;
            long currentPhyOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();
            if (currentPhyOffset > this.currentReportedOffset) {
                this.currentReportedOffset = currentPhyOffset;
                result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                if (!result) {
                    this.closeMaster();
                    log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
                }
            }

            return result;
        }

        private boolean connectMaster() throws ClosedChannelException {
            if (null == socketChannel) {
                String addr = this.masterAddress.get();
                if (addr != null) {

                    SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
                    if (socketAddress != null) {
                        this.socketChannel = RemotingUtil.connect(socketAddress);
                        if (this.socketChannel != null) {
                            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                        }
                    }
                }

                this.currentReportedOffset = HAService.this.defaultMessageStore.getMaxPhyOffset();

                this.lastWriteTimestamp = System.currentTimeMillis();
            }

            return this.socketChannel != null;
        }

        private void closeMaster() {
            if (null != this.socketChannel) {
                try {

                    SelectionKey sk = this.socketChannel.keyFor(this.selector);
                    if (sk != null) {
                        sk.cancel();
                    }

                    this.socketChannel.close();

                    this.socketChannel = null;
                } catch (IOException e) {
                    log.warn("closeMaster exception. ", e);
                }

                this.lastWriteTimestamp = 0;
                this.dispatchPosition = 0;

                this.byteBufferBackup.position(0);
                this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

                this.byteBufferRead.position(0);
                this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    if (this.connectMaster()) {

                        if (this.isTimeToReportOffset()) {
                            boolean result = this.reportSlaveMaxOffset(this.currentReportedOffset);
                            if (!result) {
                                this.closeMaster();
                            }
                        }

                        this.selector.select(1000);

                        boolean ok = this.processReadEvent();
                        if (!ok) {
                            this.closeMaster();
                        }

                        if (!reportSlaveMaxOffsetPlus()) {
                            continue;
                        }

                        long interval =
                            HAService.this.getDefaultMessageStore().getSystemClock().now()
                                - this.lastWriteTimestamp;
                        if (interval > HAService.this.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaHousekeepingInterval()) {
                            log.warn("HAClient, housekeeping, found this connection[" + this.masterAddress
                                + "] expired, " + interval);
                            this.closeMaster();
                            log.warn("HAClient, master not response some time, so close connection");
                        }
                    } else {
                        this.waitForRunning(1000 * 5);
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                    this.waitForRunning(1000 * 5);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public void shutdown() {
            super.shutdown();
            closeMaster();
        }

        // private void disableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops &= ~SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }
        // private void enableWriteFlag() {
        // if (this.socketChannel != null) {
        // SelectionKey sk = this.socketChannel.keyFor(this.selector);
        // if (sk != null) {
        // int ops = sk.interestOps();
        // ops |= SelectionKey.OP_WRITE;
        // sk.interestOps(ops);
        // }
        // }
        // }

        @Override
        public String getServiceName() {
            return HAClient.class.getSimpleName();
        }
    }
}
