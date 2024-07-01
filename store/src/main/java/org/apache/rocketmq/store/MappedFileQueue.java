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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * MappedFile 映射文件队列，也就是 MappedFile 的管理容器，对存储目录进行封装，封装的目录下存在多个内存映射文件 MappedFile。这样可以对上层使用提供无限使用的文件容量
 * todo 说明：
 *  1. MappedFile 代表一个个物理文件，而 MappedFileQueue 代表由一个 MappedFile 组成的一个连续逻辑的大文件
 *  2. 每一个 MappedFile 的命名为该文件第一条数据在整个文件序列(映射文件队列)中的物理偏移量
 */
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 一次删除文件的最大值
     */
    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**
     * 存储目录地址
     */
    private final String storePath;

    /**
     * 单个 MappedFile 大小
     */
    private final int mappedFileSize;

    /**
     * MappedFile 集合 (线程安全)
     */
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<MappedFile>();

    /**
     * 创建 MappedFile 的线程任务，主要是起到预分配 MappedFile 的作用
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 当前刷盘指针，表示该指针之前的所有数据全部持久化到磁盘，针对的是 MappedFileQueue 中所有的 MappedFile
     */
    private long flushedWhere = 0;

    /**
     * 当前提交指针，该指针位置 >= flushedWhere，针对的是 MappedFileQueue 中所有的 MappedFile
     */
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;

    /**
     * MappedFileQueue 构造方法
     *
     * @param storePath 存储目录
     * @param mappedFileSize 单个 MappedFile 大小
     * @param allocateMappedFileService 创建 MappedFile 的线程任务，主要是起到预分配 MappedFile 的作用
     */
    public MappedFileQueue(final String storePath, int mappedFileSize,
        AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                            pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /*********************************不同维度查找 MappedFile 的方法 **********************************************/

    /**
     * 根据消息存储时间戳查找 MappedFile
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        // 获取 MappedFile 数组
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return null;

        // 从 MappedFile 列表中的第一个文件开始查找
        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            // 如果 MappedFile 的最后一次修改时间戳 大于等于 给定的时间戳，则返回该 MappedFile
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        // 如果没有找到，返回最后一个 MappedFile
        return (MappedFile) mfs[mfs.length - 1];
    }

    /**
     * 获取 MappedFile 数组
     * @param reservedMappedFiles
     * @return
     */
    private Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 根据偏移量查找 MappedFile
     *
     * @param offset 物理偏移量
     * @return
     */
    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    /**
     *
     * 根据消息物理偏移量 Offset 查询 MappedFile
     * 说明：
     * 1. 使用内存映射，只要是存在于存储目录下的文件，都需要对应创建内存映射文件
     * 2. 如果不定时将已消费的消息从存储文件中删除，会造成极大的内存压力于资源浪费，因此 RocketMQ 采取定时删除存储文件的策略
     * 3. 在存储目录中，第一个文件不一定是 00000000000000000000 ， 因为该文件会在某一时刻被删除
     * todo 不管是 ConsumeQueue 还是 CommitLog ，物理偏移量都是 “左闭右开”，如：假设文件大小 40 则：
     *  1. 位于第一个文件的物理偏移量范围：[0,40)，即 0 <= offset < MappedFile.getFileFromOffset() + this.getFileSize()
     *
     * Finds a mapped file by offset.
     *
     * @param offset Offset. 物理偏移量
     * @param returnFirstOnNotFound 如果没有找到，是否返回第一个 (If the mapped file is not found, then return the first one.)
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            // 获取第一个 MappedFile
            MappedFile firstMappedFile = this.getFirstMappedFile();
            // 获取最后一个 MappedFile
            MappedFile lastMappedFile = this.getLastMappedFile();

            // 第一个 && 最后一个 MappedFile 不为空
            if (firstMappedFile != null && lastMappedFile != null) {
                // 物理偏移量不在文件中
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mappedFiles.size());

                    // 偏移量在某个文件中
                } else {
                    /**
                     * todo ？？？ 定位当前 Offset 位于第几个物理文件
                     * 注意： 这里不能使用 Offset/this.MappedFileSize，因为 RocketMQ 可能将文件名靠前的删除了，这样的话，得到的文件下标就不准确了。
                     * 比如： 通过 Offset / this.mappedFileSize 得到
                     */
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        // 取出 index 对应的 MappedFile
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }

                    // 偏移量在查询出的 MappedFile 中，直接返回
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }

                    // 遍历 MappedFile 列表，找到包含该 Offset 的 MappedFile
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }

                // 根据 Offset 没有找到对应的 MappedFile && returnFirstOnNotFound 为 true，则返回第一个
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    /**
     * 删除异常的文件
     *
     * @param offset 有效物理偏移量
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<MappedFile>();

        // 遍历 MappedFile 列表
        for (MappedFile file : this.mappedFiles) {
            // 当前文件的最大物理偏移量
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            // 如果当前文件的最大物理偏移量 大于 有效偏移量，那么说明该文件有问题，需要判断是整个文件删除还是保留有效的数据位
            if (fileTailOffset > offset) {
                // 说明文件部分有问题，保留有效的数据位
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));

                    // 整个文件无效，需要删除对应的物理文件
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        // 删除需要被删除的文件
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 删除过期的内存文件
     * 注意：会有专门的后台线程定时将内存文件刷到磁盘上
     *
     * @param files
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 按照顺序创建 MappedFile ，即从磁盘加载文件
     * @return
     */
    public boolean load() {
        File dir = new File(this.storePath);

        // 获取 MappedFile 文件列表
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            // 按照文件名升序
            Arrays.sort(files);

            // 遍历文件
            for (File file : files) {

                // 如果文件大小不匹配，则返回 false
                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    // 根据物理文件，创建对应的内存映射文件 MappedFile
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    // 将 MappedFile 的写入位置、刷盘位置、提交位置都设置为文件大小
                    // todo 不慌，在恢复过程会重置指针(只要文件非满，就会设置正确的)， {@see org.apache.rocketmq.store.MappedFileQueue.truncateDirtyFiles}
                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);

                    // 加入到 MappedFile 缓存列表中
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 获取最后一个 MappedFile
     * 规则：先尝试获取 MappedFile，没有获取到才会根据传入的 startOffset 创建一个 MappedFile
     *
     * @param startOffset
     * @param needCreate
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        // 创建文件开始 Offset，-1时，不创建
        long createOffset = -1;
        // 最后一个 MappedFile
        MappedFile mappedFileLast = getLastMappedFile();

        // 一个映射文件都不存在
        if (mappedFileLast == null) {
            // 根据传入的起始偏移量，计算出下一个文件名称
            // 方式：startOffset - 基于文件大小的余数，得到文件名，也就是文件名以存储在当前文件的第一个数据在整个文件组中的偏移量命名的，所以，第一个文件名为 0
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        // 最后一个文件已满
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        // 创建文件
        if (createOffset != -1 && needCreate) {
            // 计算文件名
            // fileName[n] = fileName[n-1] + n * mappedFileSize
            // fileName[0] = startOffset - (startOffset % mappedFileSize)
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath = this.storePath + File.separator
                + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;

            // 预分配文件 MappedFile
            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            // 设置 MappedFile 是否是第一个创建的
            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }

                // 加入 MappedFileQueue
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }

        return mappedFileLast;
    }

    /**
     * 根据起始偏移量，获取最后一个 MappedFile ，没有则创建一个
     * @param startOffset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个 MappedFile
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 获取存储文件的最小偏移量
     *
     * @return
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                // todo 返回 第一个 MappedFile 的 getFileFromOffset()
                //  因为此处的第一个 MappedFile 可能不是第一个创建的，所以，返回值可能不是 0
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 获取存储文件的最大物理偏移量
     *
     * @return 最后一个 MappedFile 的 fileFromOffset + MappedFile#wrotePosition
     */
    public long getMaxOffset() {
        // 获取最后一个 MappedFile
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            // 最后一个 MappedFile 的 fileFromOffset + MappedFile#wrotePosition
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取存储文件最大的写入位置
     * @return
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            // 最后一个 MappedFile 的起始偏移量 + 已写入的位置
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * 根据时间执行文件销毁和删除
     *
     * @param expiredTime         过期时间戳
     * @param deleteFilesInterval 删除文件间的时间间隔
     * @param intervalForcibly    强制删除时间间隔(第一次拒绝删除之后能保留文件的最大时间)
     * @param cleanImmediately    是否立即删除
     * @return
     */
    public int deleteExpiredFileByTime(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        // 从 1 到 倒数第二个文件，最后一个文件是活跃文件，其他的文件都不会被更新
        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<MappedFile>();
        // 遍历文件列表
        if (null != mfs) {
            // 从前往后遍历到倒数第 2 个文件，也就是删除最久的文件
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                // 获取文件最后修改的时间戳 + 过期时间，即是 文件的最大存活时间，过期时间，默认为 72 小时
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;

                // todo 系统当前时间 大于 文件最大存活时间 || 强制删除文件(磁盘使用超过预定的阈值)，则删除文件
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    // todo 关闭 MappedFile 的文件通道，删除物理文件
                    if (mappedFile.destroy(intervalForcibly)) {
                        // 将内存文件加入到待删除文件列表中，最后统一清除文件
                        files.add(mappedFile);
                        deleteCount++;

                        // 一次删除文件数量达到预定的最大值，则退出循环，默认一次最多删除 10 个
                        if (files.size() >= DELETE_FILES_BATCH_MAX) {
                            break;
                        }

                        // 删除一个文件后 && 不是最后一个文件，休眠 deleteFilesInterval
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        // 删除 MappedFileQueue 中的 MappedFile
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 删除 ConsumeQueue 中无效的文件
     * @param offset CommitLog 中最小的物理偏移量
     * @param unitSize CQ_STORE_UNIT_SIZE 20
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        // 某个 ConsumeQueue 下所有的 MappedFile 内存文件
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<MappedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            // 遍历从 1 到 倒数第二个文件，因为最后一个文件是活跃文件
            int mfsLength = mfs.length - 1;
            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                // ConsumeQueue 中的内容长度是 20 字节，所以需要减去 20 字节
                // 获取MappedFile 的最后一条消息条目，也就是消息索引
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    // 获取消息记录的物理偏移量
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();

                    // 如果记录的消息的物理偏移量小于 offset，则删除该文件
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                            + maxOffsetInLogicQueue + ", delete it");
                    }

                    // 文件不可用，也可以删除文件
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                // 尝试删除文件，如果删除被拒绝，那么等文件保护期过了之后，才能被删除
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        // 删除 MappedFileQueue 中无效的MappedFile
        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 刷盘
     * 1. 根据上次刷盘偏移量，找到当前待刷盘的 MappedFile 对象，最终执行 MappedByteBuffer.force() 方法 或 FileChannel.force() 方法
     * @param flushLeastPages
     * @return
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        // 根据上一次的刷盘偏移量，找到当前待刷盘的 MappedFile
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            // 消息存储时间戳
            long tmpTimeStamp = mappedFile.getStoreTimestamp();

            // 执行 MappedFile 的 force 方法，返回刷盘后的刷盘指针
            int offset = mappedFile.flush(flushLeastPages);

            // 刷盘后，新的刷盘位置是 MappedFile 的起始偏移量 + 刷盘后的刷盘指针
            long where = mappedFile.getFileFromOffset() + offset;
            // result = true ，表示当前 MappedFile 没有刷盘操作
            // 即 false 不表示刷盘失败，而是表示有数据刷盘成功
            result = where == this.flushedWhere;

            // 更新刷盘的位置
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 提交
     *
     * @param commitLeastPages
     * @return
     */
    public boolean commit(final int commitLeastPages) {
        boolean result = true;
        // 根据上一次的提交偏移量，找到对应的 MappedFile 文件
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            // 调用 MappedFile 的 commit 方法，返回提交后的提交指针
            int offset = mappedFile.commit(commitLeastPages);

            // 更新 MappedFileQueue 提交的偏移量
            long where = mappedFile.getFileFromOffset() + offset;

            // 如果 result == true , 说明当前 MappedFile 没有 Commit 操作
            // 即 false 不代表失败，而是表示有数据提交成功了，true 表示没有数据提交
            result = where == this.committedWhere;
            this.committedWhere = where;
        }

        return result;
    }

    /**
     * 获取第一个 MappedFile
     *
     * @return
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    /**
     * 获取映射的内存文件(MappedFile)大小
     * @return
     */
    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            // 遍历 MappedFile ，累加 MappedFile 的大小
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    /**
     * 尝试删除第一个文件
     *
     * @param intervalForcibly
     * @return
     */
    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            // 删除不可用的文件
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<MappedFile>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    /**
     * 关闭 MappedFileQueue
     * @param intervalForcibly
     */
    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    /**
     * 销毁 MappedFileQueue
     */
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }
}
