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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 有序消息处理服务 - 严格顺序消息（todo 这是局部的，因为只解决了 Topic 下一个 MessageQueue 的顺序，无法解决 Topic 级别的有序，除非 Topic 只有一个 MessageQueue）
 * 说明：Consumer 在严格顺序消费时，通过三把锁保证
 *  1. Broker 消息队列锁(分布式锁)
 *     - 集群模式下：Consumer 从 Broker 获的该锁后，才能进行消息拉取、消费
 *     - 广播模式下：Consumer 无需分布式锁，因为每个消费者都有相同的队列，只需要保证同一个消费队列 && 同一时刻 && 只能被一个线程消费即可
 *  2. Consumer 消息队列锁(本地锁)，Consumer 获得该锁，才能操作消息队列
 *  3. Consumer 消息处理队列锁(本地锁) Consumer 获得该锁，才能消费消息队列
 * 小结：
 *  1. ConsumeMessageOrderlyService 在消费的时候，会现获取每一个 ConsumeQueue 的锁，然后从 ProcessQueue 获取消息消费，这也就意味着 对于每一个 consumeQueue 的消息来说，消息的逻辑也是顺序的
 *  2. todo 对消息队列做什么事情之前，先申请该消息队列的锁，无论是创建消息队列拉取任务[分布式锁]、拉取消息[分布式锁]、消息消费[分布式锁、消息队列锁、消息处理队列锁]，无不如此
 */
public class ConsumeMessageOrderlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();

    /**
     * 消费任务一次运行的最大时间，默认为 60s
     */
    private final static long MAX_TIME_CONSUME_CONTINUOUSLY =
        Long.parseLong(System.getProperty("rocketmq.client.maxTimeConsumeContinuously", "60000"));

    /**
     * 消息消费者实现类
     */
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;

    /**
     * 消息消费者(消费者对外暴露类)
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 顺序消息消费监听器
     */
    private final MessageListenerOrderly messageListener;

    /**
     * 消息消费任务队列
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;

    /**
     * 消息消费线程池
     */
    private final ThreadPoolExecutor consumeExecutor;

    /**
     * 消费组名称
     */
    private final String consumerGroup;

    /**
     * 消息队列锁
     */
    private final MessageQueueLock messageQueueLock = new MessageQueueLock();

    /**
     * 提交消息消费请求到消息消费线程池的线程池
     */
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean stopped = false;

    /**
     * 创建顺序消息消费服务实例
     *
     * @param defaultMQPushConsumerImpl
     * @param messageListener
     */
    public ConsumeMessageOrderlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerOrderly messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        // todo 消息消费线程池
        // todo 由于使用的是无界队列，因此线程池中的最大线程数是无效的，故有效线程数是核心线程数
        this.consumeExecutor = new ThreadPoolExecutor(
            // 核心线程数
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            // 最大线程数
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            // 线程活跃时间
            1000 * 60,
            TimeUnit.MILLISECONDS,
            // 任务队列
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        // 定时线程池，用于延迟提交消息消费请求任务
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));
    }

    /**
     * 消费者启动时会调用该方法，在集群模式下，默认每隔 20s 执行一次锁定分配给自己的消息队列
     * todo 特别说明：
     *  1. 每个 MQ 客户端，会定时发送 LOCK_BATCH_MQ 请求，并且在本地维护获取到锁的所有队列，即在消息处理队列 ProcessQueue 中使用 locked 和 lastLockTimeStamp 进行标记
     *  2. Broker 端锁的有效时间为 60s
     */
    public void start() {
        // 集群消费
        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())) {
            // 消费方需要不断向 Broker 刷新该锁过期时间，默认配置 20s 刷新一次
            // todo 默认每隔 20s 执行一次锁定分配给自己的消息处理队列
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    // todo 锁定当前 Broker 上消费者分配到的队列（可能对应多个 Broker ）
                    ConsumeMessageOrderlyService.this.lockMQPeriodically();
                }
            }, 1000 * 1, ProcessQueue.REBALANCE_LOCK_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * 关闭消费者
     * todo 可能出现问题，机器宕机，锁没有释放，需要等待 60s （Broker 端默认 60s 锁过期），如果在这期间，消费组内其他实例分配到了该宕机机器
     *  那么就不能消费消息，造成消息消费延迟
     * @param awaitTerminateMillis
     */
    public void shutdown(long awaitTerminateMillis) {
        this.stopped = true;
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);

        // 集群模式下，unlock 所有的队列
        if (MessageModel.CLUSTERING.equals(this.defaultMQPushConsumerImpl.messageModel())) {
            this.unlockAllMQ();
        }
    }

    /**
     * unlock 所有队列
     */
    public synchronized void unlockAllMQ() {
        this.defaultMQPushConsumerImpl.getRebalanceImpl().unlockAll(false);
    }

    @Override
    public void updateCorePoolSize(int corePoolSize) {
        if (corePoolSize > 0
            && corePoolSize <= Short.MAX_VALUE
            && corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax()) {
            this.consumeExecutor.setCorePoolSize(corePoolSize);
        }
    }

    @Override
    public void incCorePoolSize() {
    }

    @Override
    public void decCorePoolSize() {
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeOrderlyContext context = new ConsumeOrderlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeOrderlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case COMMIT:
                        result.setConsumeResult(CMResult.CR_COMMIT);
                        break;
                    case ROLLBACK:
                        result.setConsumeResult(CMResult.CR_ROLLBACK);
                        break;
                    case SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        result.setConsumeResult(CMResult.CR_LATER);
                        break;
                    default:
                        break;
                }
            } else {
                result.setConsumeResult(CMResult.CR_RETURN_NULL);
            }
        } catch (Throwable e) {
            result.setConsumeResult(CMResult.CR_THROW_EXCEPTION);
            result.setRemark(RemotingHelper.exceptionSimpleDesc(e));

            log.warn(String.format("consumeMessageDirectly exception: %s Group: %s Msgs: %s MQ: %s",
                RemotingHelper.exceptionSimpleDesc(e),
                ConsumeMessageOrderlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setAutoCommit(context.isAutoCommit());
        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 提交消费请求
     * @param msgs             todo 顺序消息用不到，而是从消息队列中顺序去获取
     * @param processQueue     消息处理队列
     * @param messageQueue     消息队列
     * @param dispathToConsume 派发给消费者消费
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispathToConsume) {
        if (dispathToConsume) {
            // 创建消息消费任务
            ConsumeRequest consumeRequest = new ConsumeRequest(processQueue, messageQueue);
            // 提交到线程池消费
            this.consumeExecutor.submit(consumeRequest);
        }
    }

    /**
     * 周期性锁定当前消费者实例分配到的所有 MessageQueue
     */
    public synchronized void lockMQPeriodically() {
        if (!this.stopped) {
            this.defaultMQPushConsumerImpl.getRebalanceImpl().lockAll();
        }
    }

    /**
     * 只有锁住 MessageQueue 当前消费端才能操作消息队列
     *
     * @param mq           消息队列
     * @param processQueue 消息处理队列
     * @param delayMills   延迟执行时间
     */
    public void tryLockLaterAndReconsume(final MessageQueue mq,
                                         final ProcessQueue processQueue,
                                         final long delayMills) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                // 锁定 MessageQueue 是否成功
                // 分布式锁，向 Broker 发起锁定 MessageQueue 的请求
                boolean lockOK = ConsumeMessageOrderlyService.this.lockOneMQ(mq);
                if (lockOK) {
                    // 成功，立即提交消费请求
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 10);
                } else {
                    // 失败，3s后再试
                    ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, mq, 3000);
                }
            }
        }, delayMills, TimeUnit.MILLISECONDS);
    }

    /**
     * 尝试锁住执行的 MQ
     *
     * @param mq
     * @return
     */
    public synchronized boolean lockOneMQ(final MessageQueue mq) {
        if (!this.stopped) {
            return this.defaultMQPushConsumerImpl.getRebalanceImpl().lock(mq);
        }

        return false;
    }

    /**
     * 延迟提交消费请求
     *
     * @param processQueue 消息处理队列
     * @param messageQueue 消息队列
     * @param suspendTimeMillis 延迟时间
     */
    private void submitConsumeRequestLater(
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final long suspendTimeMillis
    ) {
        long timeMillis = suspendTimeMillis;
        if (timeMillis == -1) {
            // 默认 1000ms
            timeMillis = this.defaultMQPushConsumer.getSuspendCurrentQueueTimeMillis();
        }

        if (timeMillis < 10) {
            timeMillis = 10;
        } else if (timeMillis > 30000) {
            timeMillis = 30000;
        }

        this.scheduledExecutorService.schedule(new Runnable() {
            // 延迟一段时间提交到消息消费线程池
            @Override
            public void run() {
                ConsumeMessageOrderlyService.this.submitConsumeRequest(null, processQueue, messageQueue, true);
            }
        }, timeMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 处理顺序消费结果，并返回是否可以继续消费
     *
     * @param msgs           消息
     * @param status         消息结果状态
     * @param context        消息 context
     * @param consumeRequest 消息请求
     * @return 是否可以继续消息
     */
    public boolean processConsumeResult(
        final List<MessageExt> msgs,
        final ConsumeOrderlyStatus status,
        final ConsumeOrderlyContext context,
        final ConsumeRequest consumeRequest
    ) {
        boolean continueConsume = true;

        // 执行重试时，将不更新消费进度
        long commitOffset = -1L;

        // 自动提交，默认是自动提交
        if (context.isAutoCommit()) {
            switch (status) {
                // 考虑到 ROLLBACK、COMMIT 暂时只使用在 MYSQL 的 binlog 场景中，官方将这两状态标记为 @Deprecated
                case COMMIT:
                case ROLLBACK:
                    log.warn("the message queue consume result is illegal, we think you want to ack these message {}",
                        consumeRequest.getMessageQueue());
                // 消费成功
                case SUCCESS:
                    // 将临时消息清理掉，因为这些消息已经被消费掉了，并返回下次应该从哪里消费(实际是 consumingMsgOrderlyTreeMap 中最大消息偏移量 Offset + 1，此处是逻辑偏移量）
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    // 统计
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                // 等一会继续消费
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    // 统计
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    // todo 检查消息的重试次数，计算是否暂时挂起消费 N 毫秒(默认：1000ms)，然后继续消费，没有达到最大次数，就属于无效消费，不会更新消费进度
                    if (checkReconsumeTimes(msgs)) {
                        // todo 顺序消息消费重试
                        //      1.将消息重新放入 ProcessQueue的 msgTreeMap，然后清除掉在 consumingMsgOrderlyTreeMap 的该条消息
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);

                        // todo 2.延迟 1s 再加入到消费队列中，并结束结束本次消息消费
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());

                        // todo 3.如果执行消息重试，消息消费进度并未向前推进，故本地视为无效消费，不会更新消费进度
                        continueConsume = false;

                        // 消息重试次数 >= 允许重试的最大次数，则将消息发送到 Broker ，该消息最终会放入死信队列(DLQ) 中，RocketMQ 不会再次消息，需要人工干预
                        // todo 死信队列这种情况，提交该批消息，表示消息消费成功
                    } else {
                        // 将临时消息清理掉，因为这些消息已经被消费掉了，并返回下次应该从哪里消费(实际是 consumingMsgOrderlyTreeMap 中最大消息偏移量 Offset + 1，此处是逻辑偏移量）
                        commitOffset = consumeRequest.getProcessQueue().commit();
                    }
                    break;
                default:
                    break;
            }
            // 非自动提交
        } else {
            switch (status) {
                // 消费成功
                case SUCCESS:
                    // 统计
                    this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    break;
                // 提交
                case COMMIT:
                    // 将临时消息清理掉，因为这些消息已经被消费掉了，并返回下次应该从哪里消费(实际是 consumingMsgOrderlyTreeMap 中最大消息偏移量 Offset + 1，此处是逻辑偏移量）
                    commitOffset = consumeRequest.getProcessQueue().commit();
                    break;
                // 回滚
                case ROLLBACK:
                    // 将消息全部重新放入 ProcessQueue的 msgTreeMap，并清除掉 consumingMsgOrderlyTreeMap 的全部消息
                    consumeRequest.getProcessQueue().rollback();
                    // 延迟 1s 再加入到消费队列中，并结束结束本次消息消费
                    this.submitConsumeRequestLater(
                        consumeRequest.getProcessQueue(),
                        consumeRequest.getMessageQueue(),
                        context.getSuspendCurrentQueueTimeMillis());
                    continueConsume = false;
                    break;
                // 等一会继续消费
                case SUSPEND_CURRENT_QUEUE_A_MOMENT:
                    // 统计
                    this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), msgs.size());
                    // 检查消息的重试次数，计算是否暂时挂起消费 N 毫秒(默认：1000ms)，然后继续消费，没有达到最大次数，就属于无效消费，不会更新消费进度
                    // 实际流程与 顺序消息消费的自动提交流程是一致的
                    if (checkReconsumeTimes(msgs)) {
                        consumeRequest.getProcessQueue().makeMessageToConsumeAgain(msgs);
                        // 延迟提交消息消费请求
                        this.submitConsumeRequestLater(
                            consumeRequest.getProcessQueue(),
                            consumeRequest.getMessageQueue(),
                            context.getSuspendCurrentQueueTimeMillis());
                        continueConsume = false;
                    }
                    break;
                default:
                    break;
            }
        }

        // todo 消息处理队列未被废弃，并且 commitOffset >= 0 ，才可以提交消费进度
        if (commitOffset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), commitOffset, false);
        }

        return continueConsume;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 获取最大重复消费次数，默认为 -1 ，也就是 16次
     * @return
     */
    private int getMaxReconsumeTimes() {
        // default reconsume times: Integer.MAX_VALUE
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return Integer.MAX_VALUE;
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    /**
     * 检查是否达到消息重试次数，从而计算是否要暂时挂起消费(过一段时间继续消费)
     * 不再消费的条件：消息的重试次数 >= 最大允许的重试次数 && 发回 Broker 成功
     *
     * @param msgs
     * @return
     */
    private boolean checkReconsumeTimes(List<MessageExt> msgs) {
        // 是否暂停消息，默认：不暂停
        boolean suspend = false;
        if (msgs != null && !msgs.isEmpty()) {
            for (MessageExt msg : msgs) {
                // 消息重试次数 大于等于 最大允许的重试次数
                if (msg.getReconsumeTimes() >= getMaxReconsumeTimes()) {

                    // 设置重试次数
                    MessageAccessor.setReconsumeTime(msg, String.valueOf(msg.getReconsumeTimes()));

                    // todo 达到重试上限，将消息发回 Broker，放入死信队列，因为严格顺序消费时，不能发回 Broker 重试
                    if (!sendMessageBack(msg)) {
                        // 发送失败，暂停消费
                        suspend = true;
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                    }

                    // 消息重试次数 小于 最大允许的重试次数
                } else {
                    // 暂停消费
                    suspend = true;
                    // 累计重试次数
                    msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                }
            }
        }
        return suspend;
    }

    /**
     * 发回消息
     * 消息发回 Broker 后，对应的消息队列是死信队列
     *
     * @param msg 消息
     * @return 是否发送成功
     */
    public boolean sendMessageBack(final MessageExt msg) {
        try {
            // max reconsume times exceeded then send to dead letter queue.
            // todo 创建消息，注意 Topic 为重试Topic，在处理重试消息过程中，会判断是否达到最大重试次数，达到则放入死信队列
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            // 消息Id 不变
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);
            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());
            // 消息的重试 Topic 对应的原始 Topic
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            // 消息的重试次数
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes()));
            // 最大允许重试次数
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            // 清理事务消息属性
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);
            // 设置延迟级别
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            // 发送消息
            // todo 以普通消息的形式发送重试消息
            this.defaultMQPushConsumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer().send(newMsg);
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    public void resetNamespace(final List<MessageExt> msgs) {
        for (MessageExt msg : msgs) {
            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    /**
     * 消息请求任务
     * todo 特别说明：
     *  1. 并发消息消费：消费线程池中的线程可以并发对同一个消息队列的消息进行消费
     *  2. 顺序消息消费：消费线程池中的线程对消息队列只能串行消费。消息消费时必须成功锁定消息队列，在 Broker 端会存储消息队列的锁占用情况
     *  3. 并发消息消费服务中的消息请求任务，是直接处理拉取到消息集合(ProcessQueue中的 MsgTreeMap)
     *  4. 顺序消息消费服务中的消息请求任务，不是直接处理拉取到的消息集合(ProcessQueue中的 MsgTreeMap)，而是从消息处理队列中的消息快照中获取消息消息(ProcessQueue中的 consumingMsgOrderlyTreeMap)
     *  5. 并发消息消费服务中时一次处理完消息集合，而有序消息消费服务则超时处理消息，默认一个线程消费 1min 就歇息，不再继续消费，把剩余的消息再次提交到线程池中
     */
    class ConsumeRequest implements Runnable {

        /**
         * 消息处理队列
         */
        private final ProcessQueue processQueue;

        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;

        public ConsumeRequest(ProcessQueue processQueue, MessageQueue messageQueue) {
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

        @Override
        public void run() {
            // 如果消息处理队列废弃，直接返回，无事发生
            if (this.processQueue.isDropped()) {
                log.warn("run, the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                return;
            }

            // 根据消息队列来获取消息队列的锁对象
            // 意味着一个消费者内消息消费线程池中的线程并发度是消息队列级别的，同一个消息队列在同一时刻只能被一个线程消费，其他线程排队消费
            // todo 第一把锁
            final Object objLock = messageQueueLock.fetchLockObject(this.messageQueue);
            // JVM 加锁(这里加 JVM 锁就够了，因为每个消费者都有自己的消息队列)
            synchronized (objLock) {

                // todo (广播模式-不需要分布式锁) || (集群模式 && 消息处理队列锁有效)；
                //  广播模式的话，直接进入消费，无需使用分布式锁锁定消息队列，因为相互之间无法竞争
                if (MessageModel.BROADCASTING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                    // todo 第二把锁，其实就是 Broker 分布式锁的体现，
                        //  防御性编程，再次判断是否还持有队列的分布式锁
                    || (this.processQueue.isLocked() && !this.processQueue.isLockExpired())) {

                    // 开始消费时间
                    final long beginTime = System.currentTimeMillis();

                    // 循环
                    // continueConsume 是否继续消费
                    for (boolean continueConsume = true; continueConsume; ) {

                        // todo 每次继续消费前-判断消息处理队列是否被废弃
                        if (this.processQueue.isDropped()) {
                            log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                            break;
                        }

                        // todo 集群消费 && 消息处理队列锁未锁定
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && !this.processQueue.isLocked()) {
                            log.warn("the message queue not locked, so consume later, {}", this.messageQueue);
                            // 提交-延迟获得锁，后续会再次执行，进入到 run() 方法流程
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        // todo 集群消费 && 消息处理队列锁过期
                        if (MessageModel.CLUSTERING.equals(ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.messageModel())
                            && this.processQueue.isLockExpired()) {
                            log.warn("the message queue lock expired, so consume later, {}", this.messageQueue);
                            // 提交-延迟获得锁，后续会再次执行，进入 run() 方法流程
                            ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 10);
                            break;
                        }

                        // todo 当前周期消费时间 超过 连续时长，提交延迟消息消费请求，每一个 ConsumeRequest 消息消费任务不是以消费消息条数来计算的，而是根据消费时间(超过消费时间，把剩余消息交给其他线程消费)
                        //  默认当消费时长大于 MAX_TIME_CONSUME_CONTINUOUSLY(默认 60s) , 本次消费任务结束，由消费组内其他线程继续消费
                        long interval = System.currentTimeMillis() - beginTime;
                        if (interval > MAX_TIME_CONSUME_CONTINUOUSLY) {
                            // 默认情况下，每消费 1 分钟休息 10ms
                            ConsumeMessageOrderlyService.this.submitConsumeRequestLater(processQueue, messageQueue, 10);
                            break;
                        }

                        // 获取批量方式消费的消息条数
                        final int consumeBatchSize =
                            ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

                        // 从消息处理 队列 ProcessQueue 中的获取指定条数的消息
                        // todo 注意：从 ProcessQueue 中取出的消息，会临时存储在 ProcessQueue 的 consumingMsgOrderlyTreeMap 中
                        List<MessageExt> msgs = this.processQueue.takeMessages(consumeBatchSize);

                        // todo 处理消息的 Topic，还原为原始 Topic，主要是针对重试消息
                        defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

                        // 有消息
                        if (!msgs.isEmpty()) {
                            // 创建顺序消息消费上下文
                            final ConsumeOrderlyContext context = new ConsumeOrderlyContext(this.messageQueue);

                            ConsumeOrderlyStatus status = null;

                            // Hook
                            ConsumeMessageContext consumeMessageContext = null;
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext = new ConsumeMessageContext();
                                consumeMessageContext
                                    .setConsumerGroup(ConsumeMessageOrderlyService.this.defaultMQPushConsumer.getConsumerGroup());
                                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                                consumeMessageContext.setMq(messageQueue);
                                consumeMessageContext.setMsgList(msgs);
                                consumeMessageContext.setSuccess(false);
                                // init the consume context type
                                consumeMessageContext.setProps(new HashMap<String, String>());
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
                            }

                            // 消费开始时间
                            long beginTimestamp = System.currentTimeMillis();
                            // 消费返回类型
                            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
                            // 消息消费是否存在异常
                            boolean hasException = false;
                            try {
                                // todo 第三把锁 消息处理队列 ProcessQueue 消费锁 AQS
                                //  说明：
                                //  1) 相比 MessageQueue 锁，其粒度比较小
                                //  2) 在释放 MessageQueue 分布式锁时，可以根据消费锁快速判断是否有线程还在消费消息，只有确定没有线程消费时，才能尝试释放 MessageQueue 的分布式锁，否则可能导致两个消费者消费同一个队列消息，
                                //     同时，在释放分布式锁时，这里也不能获取到消息锁，会阻塞，直到释放了分布式锁，释放了分布式后，对应消息处理队列会被作废，丢弃。
                                this.processQueue.getConsumeLock().lock();

                                // 消息处理队列被丢弃，则直接结束本次消费进度
                                if (this.processQueue.isDropped()) {
                                    log.warn("consumeMessage, the message queue not be able to consume, because it's dropped. {}",
                                        this.messageQueue);
                                    break;
                                }

                                // todo 消费方消费消息，并返回消费结果
                                status = messageListener.consumeMessage(Collections.unmodifiableList(msgs), context);
                            } catch (Throwable e) {
                                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                                    RemotingHelper.exceptionSimpleDesc(e),
                                    ConsumeMessageOrderlyService.this.consumerGroup,
                                    msgs,
                                    messageQueue);
                                hasException = true;
                            } finally {
                                // todo 释放队列消费锁
                                this.processQueue.getConsumeLock().unlock();
                            }

                            if (null == status
                                || ConsumeOrderlyStatus.ROLLBACK == status
                                || ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                log.warn("consumeMessage Orderly return not OK, Group: {} Msgs: {} MQ: {}",
                                    ConsumeMessageOrderlyService.this.consumerGroup,
                                    msgs,
                                    messageQueue);
                            }

                            // 消费时间
                            long consumeRT = System.currentTimeMillis() - beginTimestamp;
                            // 顺序消费消息结果
                            if (null == status) {
                                // 有异常
                                if (hasException) {
                                    returnType = ConsumeReturnType.EXCEPTION;
                                // 返回 null
                                } else {
                                    returnType = ConsumeReturnType.RETURNNULL;
                                }

                                // 消费超时
                            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                                returnType = ConsumeReturnType.TIME_OUT;
                                // 消费失败，挂起消费请求一会会，稍后继续消费
                            } else if (ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT == status) {
                                returnType = ConsumeReturnType.FAILED;
                                // 消费成功但不提交
                            } else if (ConsumeOrderlyStatus.SUCCESS == status) {
                                returnType = ConsumeReturnType.SUCCESS;
                            }

                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
                            }

                            // 如果消费结果为 null ，则重置为 SUSPEND_CURRENT_QUEUE_A_MOMENT
                            if (null == status) {
                                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                            }

                            // Hook
                            if (ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                                consumeMessageContext.setStatus(status.toString());
                                consumeMessageContext
                                    .setSuccess(ConsumeOrderlyStatus.SUCCESS == status || ConsumeOrderlyStatus.COMMIT == status);
                                ConsumeMessageOrderlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
                            }

                            // 统计
                            ConsumeMessageOrderlyService.this.getConsumerStatsManager()
                                .incConsumeRT(ConsumeMessageOrderlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

                            // todo 处理消费结果 返回是否继续消费(消费失败 & 没有达到最大重新消费次数，会延时提交重新消费任务，返回 false ，不再继续消费)
                            continueConsume = ConsumeMessageOrderlyService.this.processConsumeResult(msgs, status, context, this);

                            // 消息都被消费完了，也会结束本次消费
                        } else {
                            continueConsume = false;
                        }
                    }
                    // todo 没有锁定 ConsumeQueue ，则只有等待获取到锁才能尝试消费
                } else {

                    // 判断消息处理队列是否被废弃，如果被废弃，则结束本次消费
                    if (this.processQueue.isDropped()) {
                        log.warn("the message queue not be able to consume, because it's dropped. {}", this.messageQueue);
                        return;
                    }

                    // 尝试获取 ConsumeQueue 的分布锁并再次提交消费任务
                    ConsumeMessageOrderlyService.this.tryLockLaterAndReconsume(this.messageQueue, this.processQueue, 100);
                }
            }
        }

    }

}
