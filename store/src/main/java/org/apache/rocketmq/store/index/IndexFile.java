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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

/**
 * RocketMQ存储模块中，关于索引文件操作的类。它主要负责索引文件的创建、加载、flush、销毁以及消息索引的存储和查询等功能。
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 每个 Hash 槽所占的字节数
     */
    private static int hashSlotSize = 4;

    /**
     * 每条 Index 条目占用字节数
     */
    private static int indexSize = 20;

    /**
     * 用来验证是否是一个有效的索引
     */
    private static int invalidIndex = 0;

    /**
     * Index 文件中 Hash 槽的总个数
     */
    private final int hashSlotNum;

    /**
     * Index 文件中包含的条目数
     */
    private final int indexNum;

    /**
     * 对应的映射文件
     */
    private final MappedFile mappedFile;

    /**
     * 对应的文件通道
     */
    private final FileChannel fileChannel;

    /**
     * 对应的 PageCache
     */
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * 每个 IndexFile 头部信息
     */
    private final IndexHeader indexHeader;

    /**
     * IndexFile 类的构造函数
     * 用于创建一个新的 IndexFile 对象
     * 根据文件名、Hash槽个数、Index文件包含的条目个数，结束物理偏移量、结束时间戳来初始化对象
     * 并计算文件大小，用于创建对应的 MapperFile 文件，FileChannel、MappedByteBuffer
     * @param fileName
     * @param hashSlotNum
     * @param indexNum
     * @param endPhyOffset
     * @param endTimestamp
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    /**
     * 获取对应的 MapperFile 文件名
     * @return
     */
    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    /**
     * IndexFile 头部信息加载
     */
    public void load() {
        this.indexHeader.load();
    }

    /**
     * 将内存中的数据刷新到磁盘上
     * mappedByteBuffer 中的数据刷新到磁盘上
     */
    public void flush() {
        // 记录开始时间
        long beginTime = System.currentTimeMillis();

        // 如果当前 MappedFile 对象可以被持有
        if (this.mappedFile.hold()) {
            // 更新 IndexHeader 对象中的 ByteBuffer
            this.indexHeader.updateByteBuffer();
            // 强制将 MappedByteBuffer 中的数据写入磁盘
            this.mappedByteBuffer.force();
            // 释放 MappedFile 对象
            this.mappedFile.release();
            // 输出日志执行时间
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    /**
     * 判断 IndexFile 文件是否被写满
     * @return
     */
    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    /**
     * 销毁 MappedFile ，其实就是清理掉 MapperFile 的缓存区
     * @param intervalForcibly
     * @return
     */
    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 存储消息索引
     *
     * @param key            给定的 key
     * @param phyOffset      物理偏移量
     * @param storeTimestamp 存储时间戳
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 判断 IndexFile 文件是否被写满
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 对 key 做哈希计算
            int keyHash = indexKeyHashMethod(key);
            // 取模运算，获取 Hash 槽的绝对位置
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                // 获取 Hash 槽的存储值
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // 如果 Hash 槽的值无效、或者 超出索引文件的范围(大于 IndexFile 中包含的条目数)
                // 将其置为无效
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 计算 存储时间戳与索引文件开始时间戳 之间的时间差
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();

                timeDiff = timeDiff / 1000;

                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 计算索引文件的绝对位置
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                /************* 将信息写入索引文件 *******************/
                // 将键的哈希值，物理偏移量、时间差和哈希槽的值写入索引文件
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);

                /************* 更新索引文件的头部信息 ****************/
                // 根据绝对位置，设置索引计数
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                // 设置起始物理偏移量和开始时间戳
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 如果 Hash 槽的值无效，则增加 Hash 槽计数
                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }

                // 增加索引计数
                this.indexHeader.incIndexCount();
                // 设置结束物理偏移量和结束时间戳
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            // 索引文件已经被写满，打印日志
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    /**
     * 对键进行哈希计算，并对哈希值做绝对值运算，如果小于 0 直接返回 0
     * @param key
     * @return
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            // 对 key 做 Hash 计算
            int keyHash = indexKeyHashMethod(key);
            // 计算 Hash 槽的绝对位置
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                // 获取 Hash 槽的值
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        // 从缓冲区中读取键的哈希值、物理偏移量、时间差和前一个索引的值。
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        // 根据时间范围判断是否与给定的时间范围匹配，如果匹配则将物理偏移量添加到phyOffsets列表中。
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        // 如果遍历结束或达到最大数量 maxNum，则退出循环。
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }
}
