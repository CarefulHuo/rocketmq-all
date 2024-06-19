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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 延时队列
 * 0.todo 延时消息服务不仅充当周期性(不断创建 Timer 的定时任务)扫描延时队列，还充当延时消息配置类(会对延时队列的相关配置进行持久化，如延时队列的消费进度，delayLevelTable)
 * 1.RocketMQ 实现的延时队列只能支持特定的延时时段，不能自定义延时时段，具体延时时段，可查看 @see https://rocketmq.apache.org/zh/docs/4.x/producer/04message3#%E5%BB%B6%E6%97%B6%E6%B6%88%E6%81%AF%E7%BA%A6%E6%9D%9F
 * 2.具体实现
 *   RocketMQ 发送延时消息时，先把延时消息根据延时时间段，发送到指定的队列中，RocketMQ 是把相同延迟时间段的延时消息，放到同一个队列中。
 *   然后通过一个定时器进行轮询这些队列，查看消息是否到期，如果到期就把这个消息发送到指定的 Topic 中。
 * 3.优点
 *   设计简单，把所有相同延迟时间段的消息都放到一个队列中，保证了消息到期时间的有序性，定时扫描，可以保证消息的有序性
 * 4.缺点
 *   定时器采用 Timer，Timer 采用单线程运行，如果延迟消息数量很大的情况下，单线程可能处理不过来，造成消息及时到期也没有投递的情况
 * 5.改进
 *   可以在每个延迟队列上各自采用一个 timer ，或者使用一个 timer 扫描，加一个线程池对消息进行处理，这样提高效率
 */
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    /**
     * 延迟级别，到延迟时间的映射
     */
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 延迟级别到队列消费的偏移量的映射
     */
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);

    /**
     * 存储服务
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 启动扫描延时队列的标志
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 轮询延时队列的定时器 timer
     */
    private Timer timer;

    /**
     * 消息存储
     */
    private MessageStore writeMessageStore;

    /**
     * 最大延迟级别
     */
    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.writeMessageStore = defaultMessageStore;
    }

    /**
     * 消息队列Id 到 对应的延迟级别
     * @param queueId 消息队列Id
     * @return
     */
    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    /**
     * 根据延迟级别 计算消息队列Id， queueid = delayLevel - 1
     * @param delayLevel 延迟级别
     * @return 消息队列Id
     */
    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    /**
     * @param writeMessageStore
     *     the writeMessageStore to set
     */
    public void setWriteMessageStore(MessageStore writeMessageStore) {
        this.writeMessageStore = writeMessageStore;
    }

    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Map.Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Long> next = it.next();
            int queueId = delayLevel2QueueId(next.getKey());
            long delayOffset = next.getValue();
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            stats.put(key, value);
        }
    }

    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    /**
     * 计算投递消息的时间（消费时间）
     * @param delayLevel 延迟级别
     * @param storeTimestamp 存储时间
     * @return
     */
    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        // 根据延迟级别，取出对应的的延时时间
        Long time = this.delayLevelTable.get(delayLevel);

        // 计算消息投递时间
        if (time != null) {
            return time + storeTimestamp;
        }

        // 没有对应的延时时间，默认使用 1s
        return storeTimestamp + 1000;
    }

    /**
     * 使用定时器 Timer 启动一个定时任务，把每个延时粒度封装成一个任务，然后加入到 Timer 的任务队列中
     */
    public void start() {

        // todo 使用 AtomicBoolean 进行 CAS 操作，来确保仅有一次执行 start 方法
        if (started.compareAndSet(false, true)) {

            // todo 加载延迟消息偏移量文件，默认 $user.home/store/config/delayOffset.json
            super.load();

            // 创建 Timer 定时器
            // 内部通过创建并启动一个线程，不断轮询定时器中的任务队列
            // 方法 schedule() 和方法 scheduleAtFixedRate ，如果执行任务的时间被延迟了，那么下一次任务的执行时间参考的是上一次任务“结束”时的时间来计算
            this.timer = new Timer("ScheduleMessageTimerThread", true);

            // 1. 遍历延迟级别到延迟时间的映射
            for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
                // 延迟级别
                Integer level = entry.getKey();
                // 延迟时间
                Long timeDelay = entry.getValue();
                // todo 根据延迟级别获取对应的偏移量，即 level 级别对应队列的偏移量
                Long offset = this.offsetTable.get(level);
                if (null == offset) {
                    offset = 0L;
                }

                // 每个消费队列对应单独一个定时任务进行轮询，发送到达投递时间[计划消费时间]的消息
                if (timeDelay != null) {
                    // 针对 MessageDelayLevel 配置，启动对应的 timer 开始 1s 后执行 DeliverDelayedMessageTimerTask 判断消息是否延时到期
                    this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
                }
            }

            // 2. 定时持久化延时队列的消费进度，每 10s 执行一次
            // 刷新延时队列 Topic 对应每个 queueid 的 offset 到本地磁盘(当前主 Broker)
            this.timer.scheduleAtFixedRate(new TimerTask() {

                @Override
                public void run() {
                    try {
                        if (started.get()) ScheduleMessageService.this.persist();
                    } catch (Throwable e) {
                        log.error("scheduleAtFixedRate flush exception", e);
                    }
                }
            }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
        }
    }

    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            if (null != this.timer)
                this.timer.cancel();
        }

    }

    public boolean isStarted() {
        return started.get();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    /**
     * 加载延迟消息偏移量文件
     * @return
     */
    public boolean load() {
        boolean result = super.load();
        result = result && this.parseDelayLevel();
        return result;
    }

    /**
     * 延迟消息偏移量文件的路径
     * @return
     */
    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    /**
     * 将 延迟级别到队列消费偏移量的映射 OffsetTable 解码并赋值到 offsetTable 中
     * @param jsonString 编码过的内容
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    /**
     * 将 延迟级别到队列消费偏移量的映射 OffsetTable 编码并返回
     * @param prettyFormat 是否格式化
     * @return
     */
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    /**
     * 加载 delayLevelTable 映射表
     * @return
     */
    public boolean parseDelayLevel() {
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        // 获取消息延迟级别字符串配置
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            // 获取每个延迟级别的延迟时间
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                String ch = value.substring(value.length() - 1);
                Long tu = timeUnitTable.get(ch);

                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                // 计算延迟时间
                long delayTimeMillis = tu * num;
                // 维护延迟级别到延迟时间的映射
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    /**
     * 任务：作为调度器 timer 的任务
     */
    class DeliverDelayedMessageTimerTask extends TimerTask {

        /**
         * 延迟级别，就是 delayLevelTable 中的 key 值
         */
        private final int delayLevel;

        /**
         * 延时级别对应消息队列的逻辑偏移量
         */
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        /**
         * 定时器启动之后，扫描延时队列
         */
        @Override
        public void run() {
            try {
                // 如果启动了扫描延时队列的定时器，则处理延时队列
                if (isStarted()) {
                    this.executeOnTimeup();
                }
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            long result = deliverTimestamp;

            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        /**
         * 遍历消息队列，将消息发送到 Broker
         */
        public void executeOnTimeup() {
            // 1. 根据 Topic：SCHEDULE_TOPIC_XXXX 和 queueId 获取消费队列
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            long failScheduleOffset = offset;

            if (cq != null) {
                // 2. cp (消费队列)，根据从延迟级别获取来的消息偏移量，获取消息的索引信息
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        long nextOffset = offset;
                        int i = 0;
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();

                        // 2.1 每个扫描任务主要是把对应延迟队列中所有到期的消息索引都拿出来
                        // 步长是 20 个字节，结束条件是：i< bufferCQ.getSize()
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 在 CommitLog 中的物理偏移量
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            // 消息大小
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            // 消息的 tag 标签，实际是消息的预期投递时间
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            // 判断消息的 tagsCode 是否是扩展地址
                            if (cq.isExtAddr(tagsCode)) {
                                // 如果是，则尝试从扩展单元里面获取标签代码，如果获取成功，更新 tagsCode 的值
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();

                                // 如果不是，则根据消息的存储时间戳，重新计算标签代码
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                        tagsCode, offsetPy, sizePy);
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    // 计算延迟时间
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            // 2.2 todo 修正消息的可投递时间，因为延时级别对应的延迟时间，可能会进行人为的改变(变大/变小)，这个时候不能以 tagsCode 中的预期消费时间为准
                            long now = System.currentTimeMillis();
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            // 2.3 下一个消息的逻辑偏移量(第一次的时候 i = 0)
                            // todo 推进了消息队列的偏移量
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            // 2.4 判断延时时间是否已经到达，也就是是不是到了消息的消费时间
                            long countdown = deliverTimestamp - now;

                            // 延时时间到达
                            if (countdown <= 0) {
                                // 2.5 根据物理偏移量和消息的大小，从 CommitLog 中获取对应的延时消息
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy);

                                // 2.6 获取到消息，开始进行投递
                                if (msgExt != null) {
                                    try {
                                        // 还原延迟消息的真实属性
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        // 这里做的是防御性编程，防止事务消息使用了延迟消息
                                        if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                                            log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                                                    msgInner.getTopic(), msgInner);
                                            continue;
                                        }

                                        // todo 2.6.1 投递真正的消息，此刻消息已经没有延迟消息的标志了
                                        PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.writeMessageStore
                                                .putMessage(msgInner);

                                        // 如果发送成功，则继续该延迟队列的下一个消息索引的获取
                                        if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            if (ScheduleMessageService.this.defaultMessageStore.getMessageStoreConfig().isEnableScheduleMessageStats()) {
                                                // 消息的统计信息更新
                                                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
                                                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(),
                                                    putMessageResult.getAppendMessageResult().getWroteBytes());
                                                ScheduleMessageService.this.defaultMessageStore.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());
                                            }
                                            continue;

                                            // todo 如果投递失败，就会将该偏移量重新投递执行，直到投递成功为止
                                            // todo 变量 failScheduleOffset 在一开始就被指定为当前正在执行的偏移量，所以，如果投递失败，就会将该偏移量重新投递执行，直到投递成功为止
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                            ScheduleMessageService.this.timer.schedule(
                                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                    nextOffset), DELAY_FOR_A_PERIOD);
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me



                                         */
                                        log.error(
                                            "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }

                                //todo 没有到达预期消费时间的消息，就不需要继续执行了，因为同一个队列的到期时间都是有序的，前一个没有到期，后面的就更没有，留着下次再次从这个偏移量开启遍历与判断是否到达
                            } else {
                                // 2.7 如果延迟消息不存在，直接开始扫描下一个消息，
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown); // 任务的触发时间为距离消费时间的时间差
                                // 更新 offsetTable 中对应延迟级别的偏移量
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        // 遍历完了拉取的消息索引条目，安排下次任务，继续下一次的扫描
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        // 更新 offsetTable 中对应延迟级别的偏移量
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            // todo 重新投递，投递失败的延迟消息
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        /**
         * 构建真正的消息
         * @param msgExt
         * @return
         */
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            // 延迟消息的消息标签内容--由预期投递时间转换为消息便签的HashCode值，便于消息过滤
            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());

            // todo 重试次数
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            msgInner.setWaitStoreMsgOK(false);

            // 清理掉延迟级别属性，不需要再次延迟了
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            // 还原延迟消息真实的 Topic 和 queueId 属性
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
