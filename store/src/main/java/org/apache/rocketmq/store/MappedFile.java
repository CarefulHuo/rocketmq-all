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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * RocketMQ 内存映射文件的具体实现
 * 说明：
 * 1. 存储文件名格式：00000000000000000000、00000000001073741824、00000000002147483648等文件
 * 2. 每个 MappedFile 大小统一
 * 3. 文件命名格式：fileName[n] = fileName[n-1] + MappedFileSize
 * 注意：PageCache 是操作系统分配的物理内存
 * todo 特别说明：通常有以下两种方式进行读写
 * 1. mmap + PageCache 的方式，读写消息都是走的 PageCache，这样子读写都在 PageCache 中进行，不可避免的会有锁的问题，在并发的读写操作情况下，会出现缺页中断降低，内存加锁，污染页的回写。
 * 2. DirectByteBuffer(堆外内存) + PageCache 的两层架构方式，这样可以实现读写消息分离，写入消息的时候，写到的是 DirectByteBuffer--堆外内存中，读消息走的是 PageCache（对于 DirectByteBuffer 是两步刷盘，一步是刷到 PageCache，还有一步是刷到磁盘文件中）
 *  - 好处：避免了内存中很多容易阻塞的地方，降低了时延，比如说缺页中断，内存加锁，污染页的回写
 *  - 坏处：会增加数据丢失的可能性，如果 Broker JVM 进程异常退出，提交到 PageCache 中的消息不会丢失，但存在堆外内存(DirectByteBuffer)中，还未提交到 PageCache 中的数据会丢失，但通常情况下，RocketMQ 进程退出的可能性不大。
 * 3. @see org.apache.rocketmq.store.TransientStorePool 内存锁定机制
 * todo RocketMQ 高性能读写
 * 1. 采用顺序写 + "读写分离"
 * 2. 采用零拷贝 mmap 和 write 的方法来回应 Consumer 的请求；(kafka中存在大量的网络数据持久化到磁盘和磁盘中文件通过网络发送的过程，kafka使用了 sendfile 零拷贝方式)
 * 3. 预分配 MappedFile 和 文件预热
 */
public class MappedFile extends ReferenceResource {

    /**
     * 堆内存(HeapByteBuffer)与堆外内存(DirectByteBuffer)？
     * 1. 堆内存(HeapByteBuffer)
     *  - 优点：是写在 JVM 堆的一个 Buffer，底层本质是一个数组，内容维护在 JVM 里面，读写效率会很高，并且堆内存的管理是由 JVM 的内存回收机制(GC)来管理
     * 2. 堆外内存(DirectByteBuffer)：
     *  - 优点：内容维护在内核缓存(操作系统分配的物理内存)中，跟外设(IO 设备)打交道时会快很多，因为 JVM 堆中的数据不能直接读取，而是需要把 JVM 里的数据读到一个内存块里，再从这个内存块中读取。
     *         如果使用 DirectByteBuffer，则可以省去这一步，实现zero copy(零拷贝)；外设之所以要把 JVM 堆中的数据 copy 出来再操作，不是因为操作系统不能直接操作 JVM 内存，
     *         而且因为 JVM 在进行 GC 时，会对数据进行移动，一旦出现这种问题，就会出现数据错乱的情况。
     * 3. 堆外内存实现零拷贝
     *  - HeapByteBuffer 是分配在 JVM 堆上(ByteBuffer.allocate())，后者分配在操作系统物理内存上(Buffer.allocateDirect())，JVM使用 C 库中的 malloc() 方法进行内存分配
     *  - 底层 I/O 操作需要连续的内存 (JVM堆内存容易发生GC 和 对象移动)，所以在执行 write 操作时，需要把 HeapByteBuffer 数据拷贝到一个临时的(操作系统用户态)内存空间中，会多一次额外拷贝
     *    而 DirectByteBuffer 则可以省去这个拷贝动作，这是 Java 层面上的"零拷贝"，在 Netty 中广泛应用。
     *
     * 零拷贝
     * 1. RocketMQ 默认使用 MappedByteBuffer，其底层使用了操作系统的 mmap 机制。
     * 2. MappedByteBuffer 底层使用了操作系统的 mmap 机制(将文件映射到内存中，然后直接操作内存，避免了文件I/O操作)，通过 FileChannel#map() 方法就会返回 MappedByteBuffer 对象。
     *    不过 DirectByteBuffer 默认没有直接使用 mmap 机制，此外 DirectByteBuffer 是 MappedByteBuffer 的子类
     * 3. 零拷贝的实现有两种：
     *  - mmap : 适合小数据量读写，需要 4 次上下文切换(用户态->内核态 ->用户态，用户态-->内核态-->用户态 )，3次数据拷贝(数据从磁盘-->写入到内核缓冲区；从Socket缓冲区写入到网卡；将数据从内核缓冲区拷贝到应用缓存区，以及将数据从应用缓存区拷贝到Socket缓冲区，这两次是完全没有必要的，使用零拷贝来消除这两次额外的内存拷贝-->变成只拷贝一次)
     *  - sendFile : 适合大文件传输，需要 2 次上下文切换，最少 2次数据拷贝
     * 4. 两种零拷贝实现方式对比
     *  - sendFile 方式，其实只需要 2 次上下午切换，sendFile + 网卡支持 SG-DMA 实现零拷贝技术，没有在内存层面上进行拷贝数据，也就是全程没有通过 cpu 进行数据拷贝，所有的数据都是通过 DMA 来进行传输的。
     *    如果你要对数据修改，sendFile 就不合适了，而 mmap 将磁盘文件映射到内存，支持读和写。
     *  - sendFile 相当于原汁原味的读写，直接将硬盘上的文件，传递给网卡，但这种方式不一定适用所有的场景，比如如果你需要从硬盘上读取文件，然后经过一定修改之后，再传递给网卡，就不适合用 sendFile。
     *  - 对于 RocketMQ 来说，因为 RocketMQ 将所有队列的数据写入了 CommitLog ，消费者批量消费时，需要读出来，经过应用层过滤，所以就不能利用到 sendFile + DMA 的零拷贝方式，而只能用 mmap 。
     *
     * todo
     *  transientStorePoolEnable 能缓解 PageCache 的压力背后关键如下:
     *  消息先写入到堆外内存中，该内存启用了内存锁定机制，故消息的写入是接近直接操作内存，性能是可以得到保证。消息进入到堆外内存后，后台会启动一个线程，一批一批的提交到 PageCache，
     *  即写消息时，对 PageCache 的写操作由单条写入变成了批量写入，降低了对 PageCache 的压力
     */

    /**
     * 操作系统每页大小 默认 4kb
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 当前 JVM 实例中 MappedFile 的虚拟内存
     * 全局变量，用于记录所有 MappedFile 实例已使用字节总数，根据文件大小计算的
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 当前 JVM 实例中 MappedFile 对象的个数
     * 全局变量，用于记录 MappedFile 个数，根据文件个数统计
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /************************************* start ************************************/
    /**
     * 以下三个变量的区别与联系
     * wrotePosition：写指针，写入到缓存映射文件 MappedFile 中的位置，不论是否使用堆外内存，写成功指针都会移动
     * CommittedPosition：将数据写入到 FileChannel 中的位置
     * FlushedPosition：将数据刷盘位置
     * todo 特别说明：在 MappedFile 设计中，只有提交了的数据(写入 MappedByteBuffer 或 FileChannel 中的数据) 才是安全的数据
     */

    /**
     * 当前 MappedFile 对象的当前写入位置(从 0 开始，内存映射文件的写指针)，(逻辑偏移量)
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 当前 MappedFile 对象已提交的位置(提交指针) (逻辑偏移量)
     * 注意：
     * 1. 如果开启 transientStorePoolEnable 则数据会存储在 TransientStorePool 中；
     * 2. 通过 Commit 线程将数据提交到 FileChannel 中，最后通过 Flush 线程将数据持久化到磁盘
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * (逻辑偏移量)
     * 当前 MappedFile 对象已刷盘的位置，应该满足 committedPosition >= flushedPosition
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /************************************* end ************************************/

    /**
     * 文件大小，是指单个 MappedFile 文件的大小
     */
    protected int fileSize;

    /**
     * MappedFile 文件名
     */
    private String fileName;

    /**
     * 物理文件
     */
    private File file;

    /**
     * 文件通道 jdk 中的 RandomAccessFile
     */
    protected FileChannel fileChannel;

    /**
     * 物理文件对应的内存映射 Buffer ，对应操作系统的 PageCache，即内存映射机制
     * java nio 中引入基于 MappedByteBuffer 操作大文件的方式，其读写性能极高
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 堆外内存 DirectByteBuffer
     * 说明：
     * 如果开启 transientStorePoolEnable 内存锁定，数据会先写入堆外内存(DirectByteBuffer) 然后提交到 MappedFile 创建的 FileChannel 中，并最终刷写到磁盘
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * DirectByteBuffer的缓冲池，即堆外内存池，transientStorePoolEnable=true 时开启
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 当前文件的起始物理偏移量 (对应了在 MappedFileQueue 中的偏移量)
     */
    private long fileFromOffset;

    /**
     * 最后一次写入内容的时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 是否是 MappedFileQueue 队列中的第一个文件
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * 创建并初始化 MappedFile
     *
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @throws IOException
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);

        // 从堆外内存池中取出并赋值 writeBuffer
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    /**
     * 初始化-- 没有开启堆外内存
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        // 根据文件名称创建一个真实的物理文件
        this.file = new File(fileName);
        // 文件名作为起始偏移量(物理)
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        // 确保文件所在的目录存在
        ensureDirOK(this.file.getParent());

        try {
            // 创建具备读写功能的通道，对文件 File 进行封装，用来读写文件
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();

            // todo 通过文件通道将整个文件映射到内存中，采用 mmap 方式，即采用了 MappedByteBuffer
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * 消息追加到 MappedFile
     * @param msg
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    /**
     * 将消息追加到当前的 MappedFile
     * @param messageExt
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;

        // 1. 获取当前 MappedFile 的写指针
        int currentPos = this.wrotePosition.get();

        // 2. 若 MappedFile 还可以继续写
        if (currentPos < this.fileSize) {
            // todo 根据是否开启堆外内存，选择不同的字节缓冲区，这个很重要，决定了后续刷盘的策略。
            // todo 开启堆外内存，数据写入 writeBuffer(DirectByteBuffer), 再提交堆外内存的数据到 FileChannel，然后再刷盘
            // todo 关闭堆外内存，数据直接写入 MappedByteBuffer ，然后刷盘
            // 获取写入字节缓冲区：slice() 方法用于创建一个与原对象共享的内存区，而且拥有独立的 position, limit, and mark
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            // 设置 byteBuffer 的写指针位置为 currentPos
            byteBuffer.position(currentPos);

            // 2. 根据消息类型，是批量还是单个消息，决定走不同的追加逻辑
            // todo 追加消息的逻辑，是使用传入的 AppendMessageCallback 接口内的方法
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 3. todo 更新写入指针
            // todo 注意：无论消息追加成功与否，都需要更新写入指针，还有，无论使用的是堆外内存(DirectByteBuffer)，还是文件映射(MappedByteBuffer)，写入指针都会移动
            // todo 注意：在使用堆外内存的情况下，CommittedPosition 这个提交指针，他并不是指写入到内存的指针，而是指提交到堆内存的指针
            // todo 因此在写入的时候，committedPosition < wrotePosition，在提交的时候，CommittedPosition 才会开始追赶 wrotePosition
            this.wrotePosition.addAndGet(result.getWroteBytes());

            // 4.更新消息写入的时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        // 如果写入指针大于等于文件大小，表明文件已写满
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        // 获取当前 MappedFile 的写指针
        int currentPos = this.wrotePosition.get();

        // 如果 MappedFile 还可以继续写
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                // 从当前位置开始写入
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 更新写入位置
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * 刷盘，将内存中的数据写入磁盘
     * 说明：
     * 0. flushedPosition 应该等于 MappedByteBuffer 中的指针
     * 1. 刷盘的逻辑就是调用 FileChannel 或 MappedByteBuffer 的 force 方法
     * 2. 考虑到写入性能，满足 flushLeastPages * OS_PAGE_SIZE 才进行 Flush
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        // 是否可以刷盘
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                /**
                 * todo 刷盘
                 * 1. 如果 writeBuffer 不为空，说明开启了堆外内存，那么对于刷盘来说，刷盘指针 flushedPosition 是指堆外内存的指针，应该等于上一次提交指针 committedPosition
                 * 因为上一次提交的数据就是进入 MappedByteBuffer(FileChanel) 中。注意：其实提交后 CommittedPosition 指针位置等于 wrotePosition
                 * todo 只是刷盘的时候，只能刷提交到 PageCache 的数据，所以，不能使用 wrotePosition，在未提交之前，wrotePosition > CommittedPosition
                 * 2. 如果 writeByteBuffer 为空，说明数据直接进入 MappedByteBuffer(FileChanel) 中，那么 wrotePosition 指针位置代表的是 MappedByteBuffer 的指针位置
                 */
                // 当前需要刷盘的最大位置
                int value = getReadPosition();

                // 刷盘逻辑，就是调用 fileChannel 或 mappedByteBuffer 的 force() 方法，将内存数据写入到磁盘
                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                // 更新刷盘指针位置为 value
                // todo
                //  1. 开启堆外内存，刷盘位置是 committedPosition
                //  2. 未开启堆外内存，刷盘位置是 wrotePosition
                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }

        // 返回已刷盘的位置
        return this.getFlushedPosition();
    }

    /**
     * 提交
     *
     * 说明:
     * 考虑到写入性能，满足 commitLeastPages * OS_PAGE_SIZE 才进行 Commit
     * @param commitLeastPages
     * @return
     */
    public int commit(final int commitLeastPages) {
        // 如果 未开启 writeBuffer，那么 直接将 wrotePosition 视为 committedPosition
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        // 是否允许提交
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                // 提交数据到 FileChannel
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        // 当前 MappedFile 的写入指针位置
        // todo 消息在写入成功后会更新 wrotePosition ，使用堆外内存的情况下，wrotePosition > committedPosition
        int writePos = this.wrotePosition.get();
        // 当前 MappedFile 上一次 提交指针位置
        int lastCommittedPosition = this.committedPosition.get();

        // 如果当前 MappedFile 的写入指针位置 - 上一次提交指针位置 > commitLeastPages，那么就提交数据到 FileChannel
        if (writePos - lastCommittedPosition > commitLeastPages) {
            try {
                // 这里使用 slice 方法，主要是用的同一块内存，但是是单独的指针
                ByteBuffer byteBuffer = writeBuffer.slice();

                // 设置缓存区位置为 上一次提交指针位置
                byteBuffer.position(lastCommittedPosition);

                // todo 设置缓冲区限制最大位置为 当前 MappedFile 的写入指针位置
                byteBuffer.limit(writePos);

                // todo 将 committedPosition 指针到 wrotePosition 的数据复制(写入)到 FileChannel 中，即将上一次 committedPosition 到 wrotePosition 的数据写入到 FileChannel 中
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);

                // todo 更新当前 MappedFile 的已刷盘指针位置为 当前 MappedFile 的写入指针位置，即 committedPosition == wrotePosition
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否允许刷盘
     * 1. MappedFile 文件已经被写满
     * 2. flushLeastPages > 0 && 未 Flush 页超过 flushLeastPages
     * 3. flushLeastPages == 0 && 有新写入部分
     * @param flushLeastPages
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        // 当前 MappedFile 的已刷盘位置
        int flush = this.flushedPosition.get();
        // 当前 MappedFile 的写入指针
        int write = getReadPosition();

        // 如果 MappedFile 文件满了，则返回 true
        if (this.isFull()) {
            return true;
        }

        // 刷盘至少需要的页数，如果大于 0，那么 未刷盘的页数 >= flushLeastPages，返回 true
        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        // 当前 MappedFile 的写入指针 > 已刷盘位置，返回 true
        return write > flush;
    }

    /**
     * 是否允许提交
     * 1. 当前 MappedFile 文件满了
     * 2. commitLeastPages > 0 && 未提交页数 >= commitLeastPages，返回 true
     * 3. commitLeastPages == 0 && 有未提交的部分(存在脏页)，返回 true
     *
     * @param commitLeastPages
     * @return
     */
    protected boolean isAbleToCommit(final int commitLeastPages) {
        // 当前 MappedFile 的已提交位置
        int flush = this.committedPosition.get();
        // 当前 MappedFile 的写入指针
        int write = this.wrotePosition.get();

        // 如果 MappedFile 文件满了，则返回 true
        if (this.isFull()) {
            return true;
        }

        // 提交至少需要的页数，如果大于 0，那么 未提交的页数 >= commitLeastPages，返回 true
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        // 当前 MappedFile 的写入指针 > 已提交位置，返回 true，说明存在未提交的数据(脏页)
        // 脏页是指内存中内容已经被修改的数据
        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    /**
     * 从 pos 偏移量读取 size 长度的内容
     * @param pos
     * @param size
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        // 获取当前 MappedFile 的写入指针位置
        int readPosition = getReadPosition();

        // 如果 pos + size <= 当前 MappedFile 的写入指针位置，也就是读取的数据在MappedFile 的有效范围内
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                // 从 pos 位置读取 size 长度的内容
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 根据偏移量获取对应范围内的数据，即从当前 MappedFile 传入偏移量开始读取 pos ~ (readPosition - pos) 范围内的数据
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        // 获取当前 MappedFile 文件的写入指针位置
        int readPosition = getReadPosition();

        // pos 偏移量在当前 MappedFile 的有效范围内
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                // 从 pos 偏移量开始读取长度为 (readPosition - pos) 的数据
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                // 从 pos 位置开始读取数据
                byteBuffer.position(pos);
                // 读取数据的长度为 当前 MappedFile 的写入指针位置 - pos
                int size = readPosition - pos;
                // byteBufferNew 保存的就是目标数据
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 清理文件的映射内存并进行相应的计数器
     * 1. 首先检查当前文件是否可用，如果可用则返回false，表示无法清理。
     * 2. 然后检查当前文件是否已经清理过，如果是则返回true，表示无需重复清理。
     * 3. 如果以上两个条件都不满足，则执行清理操作，包括清理mappedByteBuffer和更新映射文件计数器。
     * 4. 最后返回true，表示清理操作完成
     * @param currentRef
     * @return
     */
    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * 销毁 MappedFile 封装的文件通道和物理文件
     *
     * @param intervalForcibly 拒绝被销毁的最大存活时间
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        // 1. 关闭 MappedFile 并尝试释放资源
        this.shutdown(intervalForcibly);

        // 2. 判断是否清理完成，完成标准是 引用次数 <= 0 && cleanupOver == true
        if (this.isCleanupOver()) {
            try {
                // 3. 关闭文件通道
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();

                // 4. 删除物理文件
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * 当前 MappedFile 的最大有效数据的位置，即写入指针的位置
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        /**
         * 1. 如果 writeBuffer 为空，说明是直接写入 MappedByteBuffer ，直接返回 wrotePosition
         * 2. 如果 writerBuffer 不为空，说明开启了堆外内存，需要返回 committedPosition 提交指针
         */
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 文件预热，把当前映射的文件，每一页遍历，写入一个 0字节
     *
     * @param type  刷盘类型 {@link FlushDiskType}
     * @param pages 需要预热的页数
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            // 当刷新磁盘类型为同步时强制刷新
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        // 该方法内部：调用了 mlock 和 madvise(MADV_WILLNEED) 两个方法
        // mlock：锁定内存区域，以防止该区域被交换到磁盘。锁定的内存区域从指针地址开始，大小为文件大小
        // madvise 对内存区域进行预读取操作，以提高后续访问速度。预读取的内存从指针地址开始，大小为文件大小
        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
