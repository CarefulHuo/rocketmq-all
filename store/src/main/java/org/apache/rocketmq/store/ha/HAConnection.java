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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * Master-Slave 网络连接对象。负责主从数据同步逻辑，每一个从服务器都对应一个该对象
 * todo 说明：
 * 主服务器在收到从服务器的连接请求后，会将主从服务器的连接 socketChannel 封装成 HAConnection 对象，实现主服务器与从服务器的读写操作
 */
public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 关联的 HAService 实现类
     */
    private final HAService haService;

    /**
     * 网络 socket 通道
     */
    private final SocketChannel socketChannel;

    /**
     * 客户端连接地址
     */
    private final String clientAddr;

    /**
     * 高可用主节点网络写实现类 - 写线程任务
     * todo 即服务端向从服务器写数据服务类
     */
    private WriteSocketService writeSocketService;

    /**
     * 高可用主节点网络读实现类 - 读线程任务
     * todo 即服务端从从服务器读数据服务类
     */
    private ReadSocketService readSocketService;

    /**
     * 记录从服务器请求拉取消息的偏移量，只在首次(含主服务器重启) 主服务传输数据时确定从哪里开始传递消息
     */
    private volatile long slaveRequestOffset = -1;

    /**
     * 记录当前从服务器反馈已拉取完成的消息偏移量
     */
    private volatile long slaveAckOffset = -1;

    /**
     * 创建高可用连接对象
     * @param haService HAService 对象
     * @param socketChannel 从节点连接主节点的 Socket
     * @throws IOException
     */
    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;

        // 主节点收到从节点的 Socket 连接
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);

        // 创建向从服务器写数据的线程任务
        this.writeSocketService = new WriteSocketService(this.socketChannel);

        // 创建向从服务器读数据的线程任务
        this.readSocketService = new ReadSocketService(this.socketChannel);

        // Master 维护的连接数( Slave 的个数)
        this.haService.getConnectionCount().incrementAndGet();
    }

    /**
     * 启动 向从服务器写数据的线程 和 从从服务器读数据的线程
     */
    public void start() {
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * 服务端从从服务器读数据线程任务
     */
    class ReadSocketService extends ServiceThread {

        /**
         * 网络读缓存区大小 默认 1mb
         */
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;

        /**
         *  NIO 网络事件选择器
         */
        private final Selector selector;

        /**
         * 网络通道，用于读写的 Socket 通道
         */
        private final SocketChannel socketChannel;

        /**
         * 网络读写缓存区，默认为 1mb
         */
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);

        /**
         * ByteBufferRead 当前处理指针
         */
        private int processPosition = 0;

        /**
         * 上次读取数据的时间戳
         */
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        /**
         * 构造方法
         * @param socketChannel 网络 Socket 通道
         * @throws IOException
         */
        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // todo 将该网络通道注册在事件选择器上，并注册网络读事件，用于读取从服务器发送的复制请求
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        /**
         * 每隔 1s 处理一次读就绪事件，每次读请求调用其 processReadEvent 方法来解析从服务器的拉取请求
         */
        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 1. 事件阻塞事件
                    this.selector.select(1000);

                    // 2. 处理来自从服务器的读事件
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    // Socket 通道默认 20s 没有读事件发生，则断开该连接
                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            /*执行到这里，说明主服务器和从服务器的连接已经断开，*/

            this.makeStop();

            writeSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * 处理来自从服务器的读事件
         *  1. 主要根据从服务节点上报的已同步消息的物理偏移量，主服务节点重试唤醒同步复制的情况下等待的线程；
         *  2. 在主服务器首次传输数据时，根据从服务节点上报的已同步消息的物理偏移量确定从哪里开始传输消息
         * @return
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            // 1. 如果 ByteBufferRead 没有剩余空间，说明该 position == limit == capacity
            if (!this.byteBufferRead.hasRemaining()) {
                // 设置 position = 0 , limit = capacity 即重置 ByteBufferRead
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            // NIO 网络读的常规方法，由于 NIO 是非阻塞的，一次网络读写的字节大小不确定，一般都会尝试多次读取。知道 ByteBufferRead 没有剩余空间，则跳出循环
            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 从网络通道中读取从服务器发来的数据到 byteBufferRead
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;

                        // 更新读取数据的时间
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();

                        // 如果读取的字节大于 0 并且本次读取到的内容大于等于8 ，表明收到从服务器一条拉取消息请求
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {

                            // 读取从服务器已拉取偏移量
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            long readOffset = this.byteBufferRead.getLong(pos - 8);

                            // 更新 byteBufferRead 当前处理指针
                            this.processPosition = pos;

                            // 更新从服务器反馈已拉取完成的消息偏移量
                            HAConnection.this.slaveAckOffset = readOffset;

                            // 如果 slaveRequestOffset < 0 ，则更新 slaveRequestOffset 为 readOffset (从服务器已拉取偏移量)
                            // 只在首次使用
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            // todo 由于有新的从服务器反馈拉取偏移量，那么服务端会尝试通知某些由于同步等待主从复制结果而阻塞的消息发送者线程。因为如果消息发送使用同步方式，需要等待将消息复制到从服务器，然后才返回
                            // todo 其实是通过更新 Push2SlaveMaxOffset 的方式来实现的，看看是否达到复制成功的标准
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }

                        // 如果读取到的字数等于 0 ，则重复三次，否则结束本次读请求处理
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }

                        // 如果读取到字节数小于 0 ，表示链接处于半关闭状态，返回 false，后续会断开该连接
                    } else {
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }

                    // 异常，返回 false
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 服务端向从服务器写数据线程任务
     */
    class WriteSocketService extends ServiceThread {

        /**
         * NIO 网络事件选择器
         */
        private final Selector selector;

        /**
         * 网络 Socket 通道
         */
        private final SocketChannel socketChannel;

        /**
         * 消息头长度，消息物理偏移量 + 消息长度
         */
        private final int headerSize = 8 + 4;

        /**
         * 消息头缓存区 8 + 4 字节
         */
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);

        /**
         * 接下来消息传输的物理偏移量
         */
        private long nextTransferFromWhere = -1;

        /**
         * 根据 nextTransferFromWhere 查找消息的结果
         */
        private SelectMappedBufferResult selectMappedBufferResult;

        /**
         * 上一次数据是否传输完毕
         */
        private boolean lastWriteOver = true;

        /**
         * 上次写入消息的时间戳
         */
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;

            // 注册写事件
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 1. 事件选择，每次事件处理完成后等待 1s
                    this.selector.select(1000);

                    // 2. 如果 slaveRequestOffset 为 -1 ，说明 Master 还未收到从服务器的拉取请求，放弃本次的事件处理
                    // todo 注意：slaveRequestOffset 在收到从服务器拉取请求时更新(HAConnection#ReadSocketService())，该值是从服务器已写入消息最大物理偏移量(非刷盘物理量)
                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }

                    // 3. 如果 nextTransferFromWhere 为 -1，表示：todo 主服务器初次(含重启的情况)进行数据传输，需要计算需要传输的物理偏移量
                    if (-1 == this.nextTransferFromWhere) {

                        // 如果 slaveRequestOffset 为 0 ，表示从服务器还没写入主服务器的消息(因为从服务器的最大偏移量为 0)，这种情况从主服务器的 CommitLog 文件最大偏移量所在文件的起始
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();

                            // 最大偏移量对应的文件的起始偏移量
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;

                            // 否则，根据从服务器的拉取请求偏移量开始传输
                        } else {
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    /**
                     * todo 说明：
                     *   1. 除了首次主服务根据从服务器的拉取偏移量同步消息，后续都是主服务器根据 nextTransferFromWhere 不断的向从服务器发送消息
                     *   2. 从服务器收到消息回送后，会 ack 上报，并且从服务器还会进行超时心跳上报复制进度，这两个对于主服务器都是一致的处理逻辑
                     */

                    // 4. 判断上次写事件是否已将信息全部写入客户端

                    // 4.1 已全部写入，那么判断是否需要发送心跳
                    if (this.lastWriteOver) {

                        // 判断当前系统与上次最后写入的时间间隔是否大于 HA 心跳检测时间
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        // 大于的话，需要发送一个心跳包
                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // 心跳包 header 的长度为 12 字节：待传输的消息物理偏移量起始位置(对应的是写指针的位置)，消息长度默认为 0，表示本次数据包为心跳包，避免长连接因空闲而被关闭
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            // 发送心跳
                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }

                        // 4.2  如果上次数据未写完，则继续传输上一次的数据，然后再次判断是否传输完成，如果消息还是未全部传输，则结束本次事件处理
                        // 待下次写事件到达后，继续将未传输完的数据先写入消息从服务器
                    } else {
                        // 继续传输
                        this.lastWriteOver = this.transferData();
                        // 再次判断是否完成，如果消息还未全部传输，则结束此次事件处理，待下次事件到达后，继续将未传输完的数据先写入消息从服务器
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 5 todo 传输消息到从服务器

                    // 5.1 根据从服务器请求的待拉取偏移量，查找对应文件中从偏移量之后所有的可读消息(一个 MapperFile)
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);

                    // 5.2 如果匹配到消息
                    // HA 一批次传输消息最大字节通过 haTransferBatchSize 来设置，最大为 32 k
                    if (selectResult != null) {

                        // todo 判断获取的消息总长度是否大于配置的高可用传输一次同步任务最大传输的字节数，默认 32k，如果大于则使用最大字节数
                        // todo 但是，这就意味着从服务器收到的信息也会包含不完整的信息，从服务如果收到不完整的先不处理，存到备份缓存区，等待更多的数据到达
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;

                        // todo 累加主服务器传输的物理偏移量，作为下次从哪里拉取消息的
                        this.nextTransferFromWhere += size;

                        // 消息
                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        /***************************构建消息头************************/
                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);

                        // todo 传给从服务器的 CommitLog 偏移量，表示传送过去的消息是从这个偏移量开始拉取的，该值一般就是从服务器传过来的复制消息的偏移量(从服务器中 CommitLog 最大偏移量)
                        // 该值传到从服务器会做校验，如果发现该值和已经保存的 CommitLog 的最大偏移量不同，会忽略本次同步，必须要要保证一致才能从对应的位置开始写
                        this.byteBufferHeader.putLong(thisOffset);
                        // 消息大小
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        /*********************************构建消息体**************************/
                        // 同步消息给从服务器
                        this.lastWriteOver = this.transferData();

                        // 5.3 没有查询到匹配消息，则线程等待 100ms ，然后继续重试
                        // todo 注意：没有查到消息，不会更新拉取消息的位置
                    } else {

                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            /*************************执行到这里，说名同步消息给从节点的线程任务已经结束了，开始处理后续清理和释放资源的工作********************************/

            // 从等待的线程集合中，删除当前线程
            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            // 释放掉 selectMappedBufferResult 资源
            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            // 停止当前 HAConnection 和 readSocketService 服务
            this.makeStop();
            readSocketService.makeStop();

            // 从 haService 中删除 HAConnection 即当前连接
            haService.removeConnection(HAConnection.this);

            // 取消 selectKey
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            // 关闭 selector 和 SocketChannel
            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            // 打印服务结束的日志
            HAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 传输数据到从服务器，注意，传输的内容包含两个部分
         * - 数据头
         * - 数据体
         * @return
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header 写入头
            while (this.byteBufferHeader.hasRemaining()) {

                // 写入数据到从服务器
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            // 如果为空，就是心跳消息
            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body 写入消息体
            if (!this.byteBufferHeader.hasRemaining()) {
                // 消息内存是否还有
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            // 消息头 && 消息体都没有数据了，表示传输完成
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            // 释放掉 selectMappedBufferResult 资源
            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
