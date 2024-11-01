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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.CMResult;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * 并发消费消息
 */
public class ConsumeMessageConcurrentlyService implements ConsumeMessageService {
    private static final InternalLogger log = ClientLogger.getLog();
    private final DefaultMQPushConsumerImpl defaultMQPushConsumerImpl;
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 监听器
     */
    private final MessageListenerConcurrently messageListener;

    /**
     * 消费线程池队列
     */
    private final BlockingQueue<Runnable> consumeRequestQueue;

    /**
     * 消费线程池，消费任务 ConsumeRequest 提交到该线程池运行
     */
    private final ThreadPoolExecutor consumeExecutor;

    /**
     * 消费组名称
     */
    private final String consumerGroup;

    /**
     * 添加消费任务到 消费线程池 的定时线程池
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * 定时执行过期消息清理的线程池
     */
    private final ScheduledExecutorService cleanExpireMsgExecutors;

    /**
     * 会在 DefaultMQPushConsumerImpl#start() 方法中创建
     * @see org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl#start()
     *
     * @param defaultMQPushConsumerImpl 消息消费者内部实现类
     * @param messageListener    消息监听器
     */
    public ConsumeMessageConcurrentlyService(DefaultMQPushConsumerImpl defaultMQPushConsumerImpl,
        MessageListenerConcurrently messageListener) {
        this.defaultMQPushConsumerImpl = defaultMQPushConsumerImpl;
        // 消费者设置的监听器
        this.messageListener = messageListener;

        this.defaultMQPushConsumer = this.defaultMQPushConsumerImpl.getDefaultMQPushConsumer();
        this.consumerGroup = this.defaultMQPushConsumer.getConsumerGroup();

        // todo 消费消息任务队列 - 无界队列
        this.consumeRequestQueue = new LinkedBlockingQueue<Runnable>();

        // todo 消息消费线程池
        this.consumeExecutor = new ThreadPoolExecutor(
            this.defaultMQPushConsumer.getConsumeThreadMin(),
            this.defaultMQPushConsumer.getConsumeThreadMax(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.consumeRequestQueue,
            new ThreadFactoryImpl("ConsumeMessageThread_"));

        // 添加消息到消息消费线程池的线程池
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("ConsumeMessageScheduledThread_"));

        // 清理过期消息的线程池
        this.cleanExpireMsgExecutors = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CleanExpireMsgScheduledThread_"));
    }

    /**
     * 启动清理过期消息线程池，默认执行间隔是 15 分钟
     */
    public void start() {
        this.cleanExpireMsgExecutors.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                cleanExpireMsg();
            }

        }, this.defaultMQPushConsumer.getConsumeTimeout(), this.defaultMQPushConsumer.getConsumeTimeout(), TimeUnit.MINUTES);
    }

    public void shutdown(long awaitTerminateMillis) {
        this.scheduledExecutorService.shutdown();
        ThreadUtils.shutdownGracefully(this.consumeExecutor, awaitTerminateMillis, TimeUnit.MILLISECONDS);
        this.cleanExpireMsgExecutors.shutdown();
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
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize < this.defaultMQPushConsumer.getConsumeThreadMax())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // + 1);
        // }
        // log.info("incCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public void decCorePoolSize() {
        // long corePoolSize = this.consumeExecutor.getCorePoolSize();
        // if (corePoolSize > this.defaultMQPushConsumer.getConsumeThreadMin())
        // {
        // this.consumeExecutor.setCorePoolSize(this.consumeExecutor.getCorePoolSize()
        // - 1);
        // }
        // log.info("decCorePoolSize Concurrently from {} to {}, ConsumerGroup:
        // {}",
        // corePoolSize,
        // this.consumeExecutor.getCorePoolSize(),
        // this.consumerGroup);
    }

    @Override
    public int getCorePoolSize() {
        return this.consumeExecutor.getCorePoolSize();
    }

    @Override
    public ConsumeMessageDirectlyResult consumeMessageDirectly(MessageExt msg, String brokerName) {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        result.setOrder(false);
        result.setAutoCommit(true);

        List<MessageExt> msgs = new ArrayList<MessageExt>();
        msgs.add(msg);
        MessageQueue mq = new MessageQueue();
        mq.setBrokerName(brokerName);
        mq.setTopic(msg.getTopic());
        mq.setQueueId(msg.getQueueId());

        ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(mq);

        this.defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, this.consumerGroup);

        final long beginTime = System.currentTimeMillis();

        log.info("consumeMessageDirectly receive new message: {}", msg);

        try {
            ConsumeConcurrentlyStatus status = this.messageListener.consumeMessage(msgs, context);
            if (status != null) {
                switch (status) {
                    case CONSUME_SUCCESS:
                        result.setConsumeResult(CMResult.CR_SUCCESS);
                        break;
                    case RECONSUME_LATER:
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
                ConsumeMessageConcurrentlyService.this.consumerGroup,
                msgs,
                mq), e);
        }

        result.setSpentTimeMills(System.currentTimeMillis() - beginTime);

        log.info("consumeMessageDirectly Result: {}", result);

        return result;
    }

    /**
     * 立即提交消息消费请求
     * @param msgs             从 Broker 拉取的消息 (Broker 是从 MessageStore 存储服务)
     * @param processQueue     消息处理队列
     * @param messageQueue     消息队列
     * @param dispatchToConsume 派发给消费者消费
     */
    @Override
    public void submitConsumeRequest(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue,
        final boolean dispatchToConsume) {

        // 批量消费的消息数 默认为 1
        // msgs 默认最多是 32 条消息
        final int consumeBatchSize = this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize();

        // 提交消息数小于等于批量消息数，直接提交消费请求
        if (msgs.size() <= consumeBatchSize) {

            // 创建消息消费任务，将消息封装到里面
            ConsumeRequest consumeRequest = new ConsumeRequest(msgs, processQueue, messageQueue);
            try {
                // 提交消息消费任务到线程池
                this.consumeExecutor.submit(consumeRequest);
            } catch (RejectedExecutionException e) {
                this.submitConsumeRequestLater(consumeRequest);
            }

            // 提交消息数大于批量消息数，进行分拆分多个消费请求
        } else {
            for (int total = 0; total < msgs.size(); ) {
                List<MessageExt> msgThis = new ArrayList<MessageExt>(consumeBatchSize);
                // 计算当前拆分请求包含的消息
                for (int i = 0; i < consumeBatchSize; i++, total++) {
                    if (total < msgs.size()) {
                        msgThis.add(msgs.get(total));
                    } else {
                        break;
                    }
                }

                // 提交拆分消费请求
                ConsumeRequest consumeRequest = new ConsumeRequest(msgThis, processQueue, messageQueue);
                try {
                    this.consumeExecutor.submit(consumeRequest);
                } catch (RejectedExecutionException e) {
                    for (; total < msgs.size(); total++) {
                        msgThis.add(msgs.get(total));
                    }

                    // 提交请求被拒绝，则将当前拆分消息 + 剩余消息提交延迟消费请求，结束拆分循环
                    this.submitConsumeRequestLater(consumeRequest);
                }
            }
        }
    }


    /**
     * 清理过期消息
     */
    private void cleanExpireMsg() {
        Iterator<Map.Entry<MessageQueue, ProcessQueue>> it =
            this.defaultMQPushConsumerImpl.getRebalanceImpl().getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue pq = next.getValue();
            // 清理过期消息
            pq.cleanExpiredMsg(this.defaultMQPushConsumer);
        }
    }

    /**
     * todo 处理消费结果
     *  特别说明：
     *  1. RocketMQ 为了保证高可用，如果 Consumer 消费消息失败(只要回调函数没有返回 CONSUME_SUCCESS) 就需要重新让消费者消费该条消息
     *  2. 消息重试的策略是什么？Broker端采用延迟消息的方式，供 Consumer 再次消费
     *  3. 更新消费进度
     *   - LocalFileOffsetStore 模式下，将 Offset(消费进度) 信息转换为 json 保存到本地文件中；
     *   - RemoteBrokerOffsetStore 模式下，OffsetTable 需要提交的 MessageQueue 的 Offset(消费进度) 信息通过 MQClientAPIImpl 提供的接口 MQClientAPIImpl#updateConsumerOffsetOneway() 提交到 Broker 进行持久化存储的。
     *   - 由于是先消费再更新 Offset，因此存在消费完成后，更新 Offset 失败，但这种情况出现问题的概率比较低，更新 Offset 只是写到缓存中，是一个简单的内存操作，出错的可能性低
     *   - 由于 Offset 先存到内存中，再由定时任务每 5s 提交一次，存在丢失的风险，比如当前 Client 宕机等，从而导致更新后的 Offset 没有提交到 Broker ，再次负载时会重复消费，因此 Consumer 的消费业务逻辑要做幂等处理
     *
     * @param status 消费结果状态，消费成功/消费失败/消费重试
     * @param context 上下文
     * @param consumeRequest 消费请求
     */
    public void processConsumeResult(
        final ConsumeConcurrentlyStatus status,
        final ConsumeConcurrentlyContext context,
        final ConsumeRequest consumeRequest
    ) {

        // 确认 index
        int ackIndex = context.getAckIndex();

        // 消息为空，直接返回
        // todo 注意：consumeRequest.getMsgs() 默认只会是一条消息。这个集合表示一次消费多少条消息，当消费失败时，说明对应的集合中的消息失败了
        if (consumeRequest.getMsgs().isEmpty())
            return;

        // 计算 ackIndex 值，consumeRequest.msgs[0 - ackIndex] 为消费成功，需要进行 ACK 确认
        switch (status) {
            // 消费成功
            case CONSUME_SUCCESS:
                // 计算 ackIndex
                if (ackIndex >= consumeRequest.getMsgs().size()) {
                    ackIndex = consumeRequest.getMsgs().size() - 1;
                }

                // 统计成功/失败数量
                int ok = ackIndex + 1;
                int failed = consumeRequest.getMsgs().size() - ok;
                this.getConsumerStatsManager().incConsumeOKTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), ok);
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(), failed);
                break;

            // 稍后消费
            case RECONSUME_LATER:
                // 设置 ackIndex = -1 为下文发送 msg back(ACK) 消息做准备
                ackIndex = -1;
                // 统计成功/失败数量
                this.getConsumerStatsManager().incConsumeFailedTPS(consumerGroup, consumeRequest.getMessageQueue().getTopic(),
                    consumeRequest.getMsgs().size());
                break;
            default:
                break;
        }

        // 针对不同的消费模式做不同的处理
        switch (this.defaultMQPushConsumer.getMessageModel()) {
            // 广播模式，无论是否消费失败，不发回消息到 Broker 重试，只是打印日志
            case BROADCASTING:
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);
                    log.warn("BROADCASTING, the message consume failed, drop it, {}", msg.toString());
                }
                break;
            // 集群模式，消费失败的消息发回到Broker ，进行重试
            case CLUSTERING:
                List<MessageExt> msgBackFailed = new ArrayList<MessageExt>(consumeRequest.getMsgs().size());

                // RECONSUME_LATER：ackIndex 为 -1，执行循环；CONSUME_SUCCESS 是不会执行循环的
                for (int i = ackIndex + 1; i < consumeRequest.getMsgs().size(); i++) {
                    MessageExt msg = consumeRequest.getMsgs().get(i);

                    // todo 回退 msg 到 Broker ，稍后进行重新消费
                    boolean result = this.sendMessageBack(msg, context);

                    // 发回消息到 Broker 失败，则加入到 msgBackFiled 集合中
                    // todo 注意：可能实际发到了 Broker ，但是 Consumer 以为发送失败了，那么就会导致重复消费
                    if (!result) {
                        // 记录消息重复消费次数
                        msg.setReconsumeTimes(msg.getReconsumeTimes() + 1);
                        msgBackFailed.add(msg);
                    }
                }

                // 发回 Broker 失败的消息即重试失败，直接提交延迟(5s)重新消费
                // todo 如果发回 Broker 成功，结果因为异常，比如网络异常，导致 Consumer 以为发回 Broker 失败，判定消费发回失败，会导致消息重复消费
                //  因此，消息消费要尽最大可能性实现幂等性
                if (!msgBackFailed.isEmpty()) {
                    // 移除发回 Broker 失败的消息，然后再次尝试消费它
                    consumeRequest.getMsgs().removeAll(msgBackFailed);
                    // 提交封装了(发回 Broker 失败的消息)的延迟消费任务，
                    this.submitConsumeRequestLater(msgBackFailed, consumeRequest.getProcessQueue(), consumeRequest.getMessageQueue());
                }
                break;
            default:
                break;
        }

        /*-------------无论消费时成功，还是失败(失败会重发到 Broker 或 发送到 Broker 失败会尝试重新消费该消息)，都会尝试更新消息进度-------------*/

        /**
         * todo 可能出现消息重复消息的风险，比如如下场景：(线程池提交消费进度)：
         *  1. 假设某一个时间点，线程池有 3 个线程在消费消息，它们消费的消息对应在 ConsumeQueue 中的偏移量关系为 t1 < t2 < t3。
         *     由于支持并发消息，如果 t3 先于 t1、t2 完成处理，那么 t3 在提交消费进度时，提交 t3 消费的消息 msg3 的消费进度吗？
         *  2. 试想：如果提交 msg3 的消费进度，此时消费端重启，在 t1,t2 没有消费对应的 msg1 、msg2 的情况下，msg1 和 msg2 就不会再被消费了，
         *     因为消费进度记录的是 msg3 的，这样就出现了 “消息丢失” 的情况
         *  3. 为了避免消息丢失，提交消息消费进度，不能以哪个消息先被消费，就提交它对应的消费进度，而是提交线程池中偏移量最小的消息的偏移量。
         *     这里也就是 t3 并不会提交 msg3 对应的消费进度，而是提交线程池中偏移量最小的消息的偏移量，也就是提交的是 msg1 的偏移量。
         *     本质上是利用 ConsumeQueue 对应的 ProcessQueue 中 msgTreeMap 属性，该属性存储的是从 ConsumeQueue 中拉取的消息，
         *     针对每个消费进度都对应的有消息，并且 msgTreeMap 是 TreeMap 结构，根据消息进度对存储的消息进行了排序，也就是此时返回的偏移量
         *     有可能不是消息本身的偏移量，而是处理队列中最小的偏移量。
         *  4. 这种提交策略，避免了 “消息丢失”，但有消息重复消费的风险
         *  5. 顺序消费不会存在这个问题，因为不是并发消费
         */

        // todo 消息完成消费(消费成功 和 消息失败但成功发回Broker的消息)，需要将其从消息处理队列中移除，同时返回 ProcessQueue 中最小的 Offset，使用这个 Offset 更新消费进度
        long offset = consumeRequest.getProcessQueue().removeMessage(consumeRequest.getMsgs());

        // 更新当前消费端的 OffsetStore 中维护的 OffsetTable 中的消费位移，OffsetTable 记录每个 MessageQueue 的消费进度
        // 注意：这里只是更新内存数据，而将 Offset 上传到 Broker 或者 Offset 持久化到本地 是由定时任务执行的。@see MQClientInstance#start() 会启动客户端相关的定时任务
        // updateOffset() 方法的最后一个参数 increaseOnly 表示单调增加，新值要大于旧值
        if (offset >= 0 && !consumeRequest.getProcessQueue().isDropped()) {
            // 更新 MessageQueue 的消费进度
            this.defaultMQPushConsumerImpl.getOffsetStore().updateOffset(consumeRequest.getMessageQueue(), offset, true);
        }

        /**
         * 补充
         * 例：多线程消费，一次拉取 10个消息，OffsetSet 从 100 到 110，如果最后一个先消费成功，第 100 还在消费中，
         *    更改 本地 Offset 时用的是 100 而不是 110，这点保证了消息不会丢失，但会造成消息重复消费的情况
         * ps:
         *   1. 不管消息消费是否返回消息成功状态，都会执行上面两步，
         *      - 消费失败了，先发回到 Broker 的 retry 队列中，如果发送成功，再从本地队列中删除掉；如果发送 Broker 失败了，就会重新放入本地队列，再次进行消费，
         *        并且删除该消息，并且消费进度 Offset 也会递增，那么此时重启，，由于消费进度已经前进，但消息并没有被消费掉，也没有发回到 Broker，消息将被丢失，这算是个 bug
         *   2. OffsetTable 存储的可能不是 MessageQueue 真正消费的 Offset 最大值，但是 consumer 拉取消息时，使用的是上一次拉取请求返回的 nextBeginOffset,
         *      并不是依据 OffsetTable ，正常情况下不会重复拉取数据，当发生宕机等异常时，与 OffsetTable 未提交宕机异常一样，需要通过业务流程来保证幂等性。
         */
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.defaultMQPushConsumerImpl.getConsumerStatsManager();
    }

    /**
     * 消费不成功，进行重发消息
     *
     * @param msg     消息
     * @param context 并发消费消息上下文
     * @return
     */
    public boolean sendMessageBack(final MessageExt msg, final ConsumeConcurrentlyContext context) {

        // 1. 注意这里，默认为 0 ，其实一直都是 0 ，其他地方没有修改，表示 RocketMQ 延迟消息的延迟级别
        int delayLevel = context.getDelayLevelWhenNextConsume();

        // todo 注意：这里没有对重试主题进行处理，在 Broker端才会进行处理(专门调用处理重试消息)
        // Wrap topic with namespace before sending back message.
        msg.setTopic(this.defaultMQPushConsumer.withNamespace(msg.getTopic()));
        try {
            // 发送给当前 MessageQueue 所在的 Broker
            this.defaultMQPushConsumerImpl.sendMessageBack(msg, delayLevel, context.getMessageQueue().getBrokerName());
            return true;
        } catch (Exception e) {
            log.error("sendMessageBack exception, group: " + this.consumerGroup + " msg: " + msg.toString(), e);
        }

        return false;
    }

    /**
     * 提交延迟消费请求 5s 后重新在消费端进行消费
     * 说明：该方法用于处理消费结果时{@link #processConsumeResult(ConsumeConcurrentlyStatus, ConsumeConcurrentlyContext, ConsumeRequest)}，
     *      存在消费失败 && 重新发回 Broker 失败的情况下，会重新提交消息消费请求，进行消息的重新消费
     *
     * @param msgs 消息列表
     * @param processQueue 消息处理队列
     * @param messageQueue 消息队列
     */
    private void submitConsumeRequestLater(
        final List<MessageExt> msgs,
        final ProcessQueue processQueue,
        final MessageQueue messageQueue
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.submitConsumeRequest(msgs, processQueue, messageQueue, true);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 提交延迟消费请求
     * 说明：用于立即提交消费请求的方法{@link #submitConsumeRequest(List, ProcessQueue, MessageQueue, boolean)}中，
     *      如果出现提交消费请求异常时，则调用{@link #submitConsumeRequestLater(ConsumeRequest)}，延迟提交消费请求
     * @param consumeRequest
     */
    private void submitConsumeRequestLater(final ConsumeRequest consumeRequest
    ) {

        this.scheduledExecutorService.schedule(new Runnable() {

            @Override
            public void run() {
                ConsumeMessageConcurrentlyService.this.consumeExecutor.submit(consumeRequest);
            }
        }, 5000, TimeUnit.MILLISECONDS);
    }

    /**
     * 消息消费请求主体
     */
    class ConsumeRequest implements Runnable {

        /**
         * 需要被消费的消息集合
         */
        private final List<MessageExt> msgs;

        /**
         * 消息处理队列
         */
        private final ProcessQueue processQueue;

        /**
         * 消息队列
         */
        private final MessageQueue messageQueue;

        public ConsumeRequest(List<MessageExt> msgs, ProcessQueue processQueue, MessageQueue messageQueue) {
            this.msgs = msgs;
            this.processQueue = processQueue;
            this.messageQueue = messageQueue;
        }

        public List<MessageExt> getMsgs() {
            return msgs;
        }

        public ProcessQueue getProcessQueue() {
            return processQueue;
        }

        /**
         * 消费消息
         */
        @Override
        public void run() {
            // todo 消费消息之前，先看看消息处理队列是否被废弃，
            //  被废弃的场景很多：拉取消息异常(消费偏移量违法，拉取消息超时、消息队列重新被负载时，分配给其他的消费者，等等)
            // 废弃的消息处理队列，不会进行消费消息
            if (this.processQueue.isDropped()) {
                log.info("the message queue not be able to consume, because it's dropped. group={} {}", ConsumeMessageConcurrentlyService.this.consumerGroup, this.messageQueue);
                return;
            }

            // 获取消息监听器，即 Consumer 中设计的回调方法
            MessageListenerConcurrently listener = ConsumeMessageConcurrentlyService.this.messageListener;

            // 消费 context 上下文
            ConsumeConcurrentlyContext context = new ConsumeConcurrentlyContext(messageQueue);

            // 消费结果状态
            ConsumeConcurrentlyStatus status = null;

            // todo 当消息会重试消息时，设置 Topic 为重试 Topic
            // todo 疑惑，重试消息主题都变了，消费者怎么拉取？因为消费者在订阅 Topic 时，还会自动订阅对应的重试主题，因此可以拉取到
            defaultMQPushConsumerImpl.resetRetryAndNamespace(msgs, defaultMQPushConsumer.getConsumerGroup());

            // Hook
            ConsumeMessageContext consumeMessageContext = null;
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext = new ConsumeMessageContext();
                consumeMessageContext.setNamespace(defaultMQPushConsumer.getNamespace());
                consumeMessageContext.setConsumerGroup(defaultMQPushConsumer.getConsumerGroup());
                consumeMessageContext.setProps(new HashMap<String, String>());
                consumeMessageContext.setMq(messageQueue);
                consumeMessageContext.setMsgList(msgs);
                consumeMessageContext.setSuccess(false);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookBefore(consumeMessageContext);
            }

            // 开始消费时间
            long beginTimestamp = System.currentTimeMillis();
            boolean hasException = false;
            ConsumeReturnType returnType = ConsumeReturnType.SUCCESS;
            try {
                // 给消息设置开始消费的时间
                if (msgs != null && !msgs.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        // todo 给拉取的每条消息都在消费前都设置 PROPERTY_CONSUME_START_TIMESTAMP 属性，表示开始消费的时间，用于判断消息消费是否超时
                        /** @see ProcessQueue#cleanExpiredMsg(DefaultMQPushConsumer) */
                        MessageAccessor.setConsumeStartTimeStamp(msg, String.valueOf(System.currentTimeMillis()));
                    }
                }

                // 回调 Consumer 中的监听回调方法，进行消费，消费拉取到的消息。
                // todo 执行业务代码中监听器的消息消费逻辑
                //  status 为使用方返回的消费结果
                status = listener.consumeMessage(Collections.unmodifiableList(msgs), context);

                // 业务方消费的时候，可能抛出异常
            } catch (Throwable e) {
                log.warn("consumeMessage exception: {} Group: {} Msgs: {} MQ: {}",
                    RemotingHelper.exceptionSimpleDesc(e),
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                // 标记消费异常
                // todo 注意：消费端出现异常，也不能导致消费线程异常退出
                hasException = true;
            }

            // todo 消费时间
            long consumeRT = System.currentTimeMillis() - beginTimestamp;

            // 解析消费返回结果类型
            if (null == status) {
                // 有异常
                if (hasException) {
                    returnType = ConsumeReturnType.EXCEPTION;
                // 返回 null
                } else {
                    returnType = ConsumeReturnType.RETURNNULL;
                }

                // 如果消费时长 大于>= 15min 说明消息消费超时
            } else if (consumeRT >= defaultMQPushConsumer.getConsumeTimeout() * 60 * 1000) {
                returnType = ConsumeReturnType.TIME_OUT;

                // 如果是稍后重新消费
            } else if (ConsumeConcurrentlyStatus.RECONSUME_LATER == status) {
                returnType = ConsumeReturnType.FAILED;

                // 消费成功
            } else if (ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status) {
                returnType = ConsumeReturnType.SUCCESS;
            }

            // Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.getProps().put(MixAll.CONSUME_CONTEXT_TYPE, returnType.name());
            }

            // todo 消费结果为 null(可能出现异常了)，也设置为稍后重新消费
            if (null == status) {
                log.warn("consumeMessage return null, Group: {} Msgs: {} MQ: {}",
                    ConsumeMessageConcurrentlyService.this.consumerGroup,
                    msgs,
                    messageQueue);
                status = ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }

            // Hook
            if (ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.hasHook()) {
                consumeMessageContext.setStatus(status.toString());
                consumeMessageContext.setSuccess(ConsumeConcurrentlyStatus.CONSUME_SUCCESS == status);
                ConsumeMessageConcurrentlyService.this.defaultMQPushConsumerImpl.executeHookAfter(consumeMessageContext);
            }

            // 统计
            ConsumeMessageConcurrentlyService.this.getConsumerStatsManager()
                .incConsumeRT(ConsumeMessageConcurrentlyService.this.consumerGroup, messageQueue.getTopic(), consumeRT);

            // 处理消费结果
            // todo 消息处理队列有效的情况下，处理消费结果，否则就不处理消费结果。
            //  这也意味着，如果消息刚好被消费了，但是消息处理队列被置为无效了，那么消费进度不会被更新，这可能导致消息重复消费,因此消息消费要尽最大可能保证消息消费幂等
            if (!processQueue.isDropped()) {
                ConsumeMessageConcurrentlyService.this.processConsumeResult(status, context, this);
            } else {
                log.warn("processQueue is dropped without process consume result. messageQueue={}, msgs={}", messageQueue, msgs);
            }
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }

    }
}
