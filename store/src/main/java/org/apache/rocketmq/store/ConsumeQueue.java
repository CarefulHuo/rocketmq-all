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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 必要性说明：
 * 1. RocketMQ 是通过订阅 Topic 消费消息，但是 CommitLog 是存储消息是不区分 Topic 的。
 * 2. 消费者通过遍历 CommitLog 去消费消息，那么效率就很低下了，所以，设计了 ConsumerQueue 作为 CommitLog 对应的索引文件
 * <p>
 * 前置说明:
 * 1. ConsumerQueue ：MappedFileQueue : MappedFile = 1 : 1 : N
 * 2. MappedFile ：00000000000000000000等文件
 * 3. MappedFileQueue：
 *  - 对 MappedFile 进行封装成文件队列，对上层提供可无限使用的文件容量
 *  - 每个 MappedFile 文件大小是统一的
 *  - 文件命名方式:  fileName[n] = fileName[n - 1] + mappedFileSize
 *  4. ConsumerQueue 存储在 MappedFile 的内容大小必须是 20B (ConsumerQueue.CQ_STORE_UNIT_SIZE)
 *  <p>
 *  <p>
 *  消息消费队列，引入的目的主要是为了提高消息消费效率，由于 RocketMQ 是基于主题 Topic 的订阅模式，消息消费时针对主题进行的，所以，要遍历 CommitLog 文件，根据 Topic 检索消息时非常低效的
 *  特别说明：
 *  1. 运行过程中，消息发送到 CommitLog 文件后，会同步将消息转发到消息队列(ConsumerQueue)
 *  2. broker 启动时，检测 CommitLog 文件与 ConsumerQueue 、index 文件中消息是否一致，如果不一致，需要根据 CommitLog 文件重新恢复 ConsumerQueue 和 index 文件
 */
public class ConsumeQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * ConsumeQueue 条目，存储在 MappedFile 的内容大小必须是 20字节 (20B)
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * RocketMQ 存储核心服务
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * ConsumeQueue 对应的 MappedFile 队列
     */
    private final MappedFileQueue mappedFileQueue;

    /**
     * ConsumeQueue 所属的 Topic
     */
    private final String topic;

    /**
     * ConsumeQueue 所属的队列ID
     */
    private final int queueId;

    /**
     * ConsumeQueue 条目的缓冲区
     */
    private final ByteBuffer byteBufferIndex;

    /**
     * ConsumeQueue 存储路径为 rocket_home/store/consume/{topic}/{queueId}
     */
    private final String storePath;
    /**
     * 默认大小为 30W 条记录，也就是 30W * 20B
     */
    private final int mappedFileSize;

    /**
     * 记录当前 ConsumeQueue 中存放的消息索引对象对应的消息最大物理偏移量 (是在 CommitLog 中)
     * todo 该属性的主要作用是判断当前 ConsumeQueue 已经保存消息索引对象对应消息的物理偏移量，和 ConsumeQueue 物理偏移量没有关系
     */
    private long maxPhysicOffset = -1;

    /**
     * 当前 ConsumeQueue 最小物理偏移量
     */
    private volatile long minLogicOffset = 0;
    private ConsumeQueueExt consumeQueueExt = null;

    /**
     * 创建并初始化消费队列
     * @param topic
     * @param queueId
     * @param storePath
     * @param mappedFileSize
     * @param defaultMessageStore
     */
    public ConsumeQueue(
        final String topic,
        final int queueId,
        final String storePath,
        final int mappedFileSize,
        final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;

        this.topic = topic;
        this.queueId = queueId;

        /**
         * ConsumeQueue 文件夹
         * 格式： ../topic/queueId
         */
        String queueDir = this.storePath
            + File.separator + topic
            + File.separator + queueId;

        /**
         * 创建 MappedFile 队列
         */
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);

        /**
         * 分配 20B 的 ByteBuffer
         */
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);

        /**
         * 是否开启一个 ConsumeQueue 的扩展文件
         */
        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
                defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    /**
     * 加载 ConsumeQueue 队列
     * @return
     */
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * 恢复 Consume 队列，
     * 默认从倒数第三个文件开始恢复，不足三个文件，从第一个开始恢复
     */
    public void recover() {
        // 1. 获取该 ConsumeQueue 的所有内存映射文件
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {

            // 2. 从队列中获取倒数第三个文件开始恢复，这应该是一个经验值
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            // 3. 获取 MappedFile 的大小
            int mappedFileSizeLogics = this.mappedFileSize;

            // 倒数第三个或第一个内存映射文件 MappedFile
            MappedFile mappedFile = mappedFiles.get(index);

            // 内存映射文件对应的 ByteBuffer
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

            // 内存映射文件-起始物理偏移量
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;

            while (true) {
                // 4. 循环验证 ConsumeQueue 包含条目的有效性(如果 Offset 大于等于 0 && size 大于 0 则表示一个有效的条目)
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    // 5.读取一个条目的内容
                    // 5.1 该条目记录的该条消息在 commitLog 中的物理偏移量
                    long offset = byteBuffer.getLong();
                    // 5.2 该条目记录的该条消息的长度
                    int size = byteBuffer.getInt();
                    // 5.3 该条目记录的该条消息的 tag 标签的 HashCode
                    long tagsCode = byteBuffer.getLong();

                    // 如果 Offset >= 0 && size > 0 则表示一个有效的条目
                    if (offset >= 0 && size > 0) {
                        // 更新 ConsumeQueue 中有效的 mappedFileOffset
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        // 更新当前 ConsumeQueue 存储消息索引对象对应消息的最大物理偏移量
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }

                        // 如果发现，不正常的条目，直接跳出循环
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }

                // 6. 如果该 ConsumeQueue 中的全部条目都有效，则继续验证下一个文件(index++)
                // 如果发现条目有问题，则跳出循环，后面的 MappedFile 也没有验证的必要
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }

            // 7. processOffset 代表了当前 ConsumeQueue 中有效的物理偏移量
            processOffset += mappedFileOffset;
            // 8. 设置MappedFileQueue 的 刷盘位置、提交位置为当前的 processOffset
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            // 9. 截断无效的 ConsumeQueue 文件，只保留到 processOffset 位置的有效文件
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    /**
     * 根据消息存储时间来查询 MappedFile 中的偏移量
     * @param timestamp
     * @return
     */
    public long getOffsetInQueueByTime(final long timestamp) {
        // 查询 MappedFile  更新时间 >= timeStamp 的 MappedFile
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);

        // 采用二分查找
        if (mappedFile != null) {
            long offset = 0;
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            long leftIndexValue = -1L, rightIndexValue = -1L;
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        long storeTime =
                            this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    if (targetOffset != -1) {

                        offset = targetOffset;
                    } else {
                        if (leftIndexValue == -1) {

                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {

                            offset = leftOffset;
                        } else {
                            offset =
                                Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp
                                    - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }

                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    /**
     * 根据 CommitLog 有效的物理偏移量，清除无效的 ConsumeQueue 数据
     * @param phyOffet
     */
    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        this.maxPhysicOffset = phyOffet;
        long maxExtAddr = 1;
        while (true) {
            // 获取最后一个 MappedFile
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                // 重置三个指针[清除无效的数据，本质上就是修改指针到有效数据的位置]
                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                // 遍历 MappedFile 中的数据
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    // 如果是第一条记录
                    if (0 == i) {
                        // 记录的消息物理偏移量达到了最大有效偏移量，那么说明整个 MappedFile 都是无效的，删除当前的这个 MappedFile
                        if (offset >= phyOffet) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;

                            // 更新三个指针的位置
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);

                            // 变更 ConsumeQueue 记录的消息的最大物理偏移量
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }

                        // 如果不是第一条数据
                    } else {

                        // 如果当前索引有效，继续判断该条目，否则直接返回即可，因为三大指针位置已经在上个循序中修改了
                        if (offset >= 0 && size > 0) {

                            // 如果 ConsumeQueue 的物理偏移量超过了最大有效物理偏移量，那么说明该条记录是无效的，直接返回即可
                            if (offset >= phyOffet) {
                                return;
                            }

                            // 更新当前 MappedFile 遍历到的位置 pos
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            // 更新三大指针位置
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);

                            // 更新 ConsumeQueue 的最大物理偏移量
                            this.maxPhysicOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            // 如果当前文件遍历完毕，说明此时遍历到的 MappedFiel 文件是有效文件，三大指针记录的是有效位置
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    /**
     * 获取 ConsumeQueue 最后一个消息的物理偏移量
     * @return
     */
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    /**
     * ConsumeQueue 刷盘
     *
     * @param flushLeastPages
     * @return
     */
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    /**
     * 删除 ConsumeQueue 中过期的文件
     * @param offset CommitLog 文件中最小的物理偏移量
     * @return
     */
    public int deleteExpiredFile(long offset) {
        // 删除过期的 MappedFile 文件
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        // 修正 ConsumeQueue 中的最小物理偏移量
        // 因为前面是根据每一个文件的最后一个消息条目决定是否删除文件的，因此对于剩余的文件中的第一个文件的最小物理偏移量，需要修正
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * 修正 ConsumeQueue 中的最小物理偏移量
     * @param phyMinOffset
     */
    public void correctMinOffset(long phyMinOffset) {
        // 获取第一个 MappedFile 文件
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // 获取当前 MappedFile 文件中从 0 ~ 已写入的位置 范围内的数据
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        // 消息物理偏移量
                        long offsetPy = result.getByteBuffer().getLong();
                        // 消息长度
                        result.getByteBuffer().getInt();
                        // 消息的 tagsCode
                        long tagsCode = result.getByteBuffer().getLong();

                        // 如果当前消息索引条目中记录的物理偏移量大于等于 CommitLog 文件中最小的物理偏移量
                        // 那么重置 ConsumeQueue 中的最小物理偏移量为 当前 MappedFile 文件的起始物理偏移量 + 当前索引条目的物理偏移量
                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    /**
     * 获取 ConsumeQueue 中最小消息的物理偏移量
     * @return
     */
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 根据 DispatchRequest 消息请求，将消息物理偏移量、长度、tagCode 写入 ConsumeQueue 队列中
     * @param request
     */
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        // 最大重试次数 30
        final int maxRetries = 30;
        // 判断当前 ConsumeQueue 是否可写
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            // 1. 消息tag 的 HashCode
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                        topic, queueId, request.getCommitLogOffset());
                }
            }

            // 2. 添加消息的位置信息
            boolean result = this.putMessagePositionInfo(
                    request.getCommitLogOffset(), // 消息在 CommitLog 中的物理偏移量
                    request.getMsgSize(), // 消息大小
                    tagsCode, // 消息 tag的 HashCode
                    request.getConsumeQueueOffset() // todo 消息在消息队列中的逻辑偏移量，为了找到在哪个 ConsumeQueue 的位置开始写
            );


            if (result) {
                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                    this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                    + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    /**
     * 添加消息的位置信息，并返回是否添加成功
     * <p>
     * 1. ConsumeQueue 每一个条目都是 20 个字节（8个字节 CommitLog 偏移量 + 4 字节消息长度 + 8 字节tag的 HashCode）
     * 2. todo 注意：是将内容追加到 ConsumeQueue 的内存映射文件中(只追加，不刷盘)，ConsumeQueue 的刷盘方式固定为异步刷盘
     * 刷盘任务启动是在 {@link DefaultMessageStore#start()}
     *
     *
     * @param offset 消息在 CommitLog 中的物理偏移量
     * @param size   消息大小
     * @param tagsCode 消息tag 的 HashCode
     * @param cqOffset 写入 ConsumeQueue 的逻辑偏移量
     * @return
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
        final long cqOffset) {

        // 如果当前消息的物理偏移量 + 大小 <= 当前 ConsumeQueue 的最大物理偏移量，则说明已经添加到 ConsumeQueue，无需重复添加
        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }

        // 1. 将一条 ConsumeQueue 条目总共 20 字节，写入 ByteBuffer 缓存区中，即写入位置信息到 ByteBuffer
        this.byteBufferIndex.flip();
        // 缓存区设置限制为 20 字节
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        // 消息在 CommitLog 中的物理偏移量
        this.byteBufferIndex.putLong(offset);
        // 消息大小
        this.byteBufferIndex.putInt(size);
        // 消息tag 的 HashCode (todo 注意：如果是延时消息时，tagCode 写入的是计划发送时间)
        this.byteBufferIndex.putLong(tagsCode);

        // 2. todo 根据消息的逻辑偏移量计算消息索引在 ConsumeQueue 中的物理偏移量开始位置
        // cqOffset=0 -> expectLogicOffset = 0
        // cqOffset=1 -> expectLogicOffset = 20
        // cqOffset=2 -> expectLogicOffset = 40
        // .. todo expectLogicOffset == MappedFile.WrotePosition() + MappedFile.getFileFromOffset(),因为写入的指针位置，就是下次写入数据的位置
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        // 3. 根据消息在 ConsumeQueue  中的物理偏移量，查找对应的 MappedFile
        // todo 如果找不到，则新建一个 MappedFile，对应的物理文件名称是 expectLogicOffset
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            // 3.1 如果文件是新建的，需要先填充前置空白占位
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                // todo 如果是第一个创建的文件，并且没有添加过消息的位置信息，设置 ConsumeQueue 的 最小物理偏移量
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);

                // 填充前置空白占位
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                    + mappedFile.getWrotePosition());
            }

            // 校验 ConsumeQueue 存储位置是否合法
            if (cqOffset != 0) {
                // 获取当前 ConsumeQueue(最后一个 MappedFile) 已经写入的物理偏移量
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                // 如果当前 ConsumeQueue 的写入物理偏移量 > 当前消息的预期偏移量，则说明 ConsumeQueue 已经添加过该消息的位置信息，无需重复添加
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                // todo 如果当前 ConsumeQueue 的写入物理偏移量 != 当前消息的预期偏移量，则说明 ConsumeQueue 存储位置出现了问题
                // todo 因为当前追加的消息索引的物理偏移量必须是上次写入的位置，因为每个索引的长度是固定的
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                        "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                        expectLogicOffset,
                        currentLogicOffset,
                        this.topic,
                        this.queueId,
                        expectLogicOffset - currentLogicOffset
                    );
                }
            }

            // 4. 重置当前 ConsumeQueue 的最大物理偏移量
            this.maxPhysicOffset = offset + size;

            // 5. 追加消息索引到 ConsumeQueue 中，添加过程是基于 MappedFile 来完成的
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    /**
     * 填充前置空白占位
     * @param mappedFile
     * @param untilWhere ConsumeQueue 需要写入的存储位置
     */
    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        // 写入前置空白占位到 ByteBuffer
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        // 循环填空
        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 根据 StartIndex 获取消息消费队列条目
     * @param startIndex 逻辑偏移量(针对 ConsumeQueue 文件组) 类似索引下标
     * @return
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;

        // 1. 通过 StartIndex * 20 得到在 ConsumeQueue 文件组中的物理偏移量
        long offset = startIndex * CQ_STORE_UNIT_SIZE;

        // 2. todo 如果该物理偏移量 < ConsumeQueue 的最小物理偏移量，则说明该消息索引不存在，则返回 null
        // 如果该物理偏移量 >= minLogicOffset ，则根据偏移量定位到具体的物理文件
        if (offset >= this.getMinLogicOffset()) {
            // 根据物理偏移量找到对应的 MappedFile
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);

            // 获取对应的数据
            if (mappedFile != null) {
                // todo 通过将该物理偏移量 % mappedFileSize 取模获取在该文件的逻辑偏移量，从该逻辑偏移量开始读取该文件所有的数据
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
                return result;
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    /**
     * 根据逻辑偏移量，获取下一个消息索引文件
     * @param index
     * @return
     */
    public long rollNextFile(final long index) {
        // 索引文件大小
        int mappedFileSize = this.mappedFileSize;

        // 一个文件，可以存储索引的个数
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;

        // 下一个文件的起始索引
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        // todo 获取消息索引的最大逻辑偏移量 ：消息索引的最大物理偏移量 / 20
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
            && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
