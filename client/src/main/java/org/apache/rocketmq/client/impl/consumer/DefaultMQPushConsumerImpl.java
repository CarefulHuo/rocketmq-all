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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * todo 重要
 * 消息消费者内部默认实现类，对外暴漏的是 DefaultMQPushConsumer
 */
public class DefaultMQPushConsumerImpl implements MQConsumerInner {
    /**
     * Delay some time when exception occur
     */
    private long pullTimeDelayMillsWhenException = 3000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_SUSPEND = 1000;
    private static final long BROKER_SUSPEND_MAX_TIME_MILLIS = 1000 * 15;
    private static final long CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND = 1000 * 30;
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 对外暴漏的消费者对象
     */
    private final DefaultMQPushConsumer defaultMQPushConsumer;

    /**
     * 每个 Consumer 的对象都持有一个 RebalanceImpl 实例，每个 RebalanceImpl 实例也只服务于一个 Consumer。
     * Consumer 负载均衡相关的操作都交给了 RebalanceImpl 对象，均衡消息队列服务，负责分配当前 Consumer 可消费的消息队列(MessageQueue)，当有新的 Consumer 的加入或移除，都会重新分配消息队列
     */
    private final RebalanceImpl rebalanceImpl = new RebalancePushImpl(this);
    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();
    private final long consumerStartTimestamp = System.currentTimeMillis();
    private final ArrayList<ConsumeMessageHook> consumeMessageHookList = new ArrayList<ConsumeMessageHook>();
    private final RPCHook rpcHook;

    /**
     * 刚启动就是此状态
     */
    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    /**
     * 客户端实例
     */
    private MQClientInstance mQClientFactory;

    /**
     * push 模式的封装，本质上还是使用 pull 模式进行消息消费
     */
    private PullAPIWrapper pullAPIWrapper;
    private volatile boolean pause = false;

    /**
     * 默认并发消费
     */
    private boolean consumeOrderly = false;

    /**
     * 消息监听器，由业务方创建消费者时传入
     */
    private MessageListener messageListenerInner;

    /**
     * 消息消费进度存储
     */
    private OffsetStore offsetStore;

    /**
     * 消息消费服务，不断消费消息，并处理消费结果
     */
    private ConsumeMessageService consumeMessageService;
    private long queueFlowControlTimes = 0;
    private long queueMaxSpanFlowControlTimes = 0;

    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer, RPCHook rpcHook) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
        this.rpcHook = rpcHook;
        this.pullTimeDelayMillsWhenException = defaultMQPushConsumer.getPullTimeDelayMillsWhenException();
    }

    public void registerFilterMessageHook(final FilterMessageHook hook) {
        this.filterMessageHookList.add(hook);
        log.info("register FilterMessageHook Hook, {}", hook.hookName());
    }

    public boolean hasHook() {
        return !this.consumeMessageHookList.isEmpty();
    }

    public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
        this.consumeMessageHookList.add(hook);
        log.info("register consumeMessageHook Hook, {}", hook.hookName());
    }

    public void executeHookBefore(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void executeHookAfter(final ConsumeMessageContext context) {
        if (!this.consumeMessageHookList.isEmpty()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                }
            }
        }
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        Set<MessageQueue> result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        if (null == result) {
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            result = this.rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
        }

        if (null == result) {
            throw new MQClientException("The topic[" + topic + "] not exist", null);
        }

        return parseSubscribeMessageQueues(result);
    }

    public Set<MessageQueue> parseSubscribeMessageQueues(Set<MessageQueue> messageQueueList) {
        Set<MessageQueue> resultQueues = new HashSet<MessageQueue>();
        for (MessageQueue queue : messageQueueList) {
            String userTopic = NamespaceUtil.withoutNamespace(queue.getTopic(), this.defaultMQPushConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, queue.getBrokerName(), queue.getQueueId()));
        }

        return resultQueues;
    }

    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public OffsetStore getOffsetStore() {
        return offsetStore;
    }

    public void setOffsetStore(OffsetStore offsetStore) {
        this.offsetStore = offsetStore;
    }

    /**
     * 拉取消息过程
     * 1. 从 Broker 拉取消息，并加入到消息处理队列 ProcessQueue
     * 2. 将拉取到的消息提交到消息消费服务(ConsumeMessageService)，执行 listener.consumeMessage() 方法消费消息
     * 3. 消费者进行消息消费，根据业务反馈是否成功消费来推动消费进度，消费成功则更新消费进度，(集群模式下)消息失败但是发送到 Broker ，也会更新消费进度
     * 注意：消息的消费进度，集群模式下保存在 Broker端，广播模式下保存在消费端(本地客户端)
     * @param pullRequest
     */
    public void pullMessage(final PullRequest pullRequest) {
        // todo 12.2.4 从 pullRequest 中获取消息处理队列 processQueue;
        final ProcessQueue processQueue = pullRequest.getProcessQueue();

        // todo 判断消息处理队列是否被废弃，被废弃，直接返回
        if (processQueue.isDropped()) {
            log.info("the pull request[{}] is dropped.", pullRequest.toString());
            return;
        }

        // todo 设置上次拉取消息的时间戳为当前时间戳，根据该值，判断 PullMessageService 是否空闲(没有拉取消息请求要处理，就会阻塞等待)
        pullRequest.getProcessQueue().setLastPullTimestamp(System.currentTimeMillis());

        //
        try {
            // 判断消费者实例是否正在运行，如果不是，则延迟拉取消息
            this.makeSureStateOK();
        } catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            // 消费者实例没有在运行，等待 3 秒后执行拉取消息请求
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            return;
        }

        // 判断 消费者实例 是否暂停
        if (this.isPause()) {
            log.warn("consumer was paused, execute pull request later. instanceName={}, group={}", this.defaultMQPushConsumer.getInstanceName(), this.defaultMQPushConsumer.getConsumerGroup());
            // 如果消费者服务暂停，等待 1 秒后执行拉取请求
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_SUSPEND);
            return;
        }

        // 消息处理队列持有的待消费消息总数
        long cachedMessageCount = processQueue.getMsgCount().get();
        // 消息处理队列持有的待处理消息总大小
        long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

        /**----------------------------流控：如果消息消费过慢产生消息堆积，会消息消费拉取流控---------------------*/

        // 从数量上进行流控
        // 如果消息处理队列持有的消息数据超过最大允许值(内存可能要满了)，默认 1000 条，触发流控，延迟 50ms 拉取消息
        if (cachedMessageCount > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                    "the cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    this.defaultMQPushConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        // 从消息大小上进行流控
        // 如果消息处理队列持有的消息总大小超过最大允许值(内存可能要满了)，默认 100MB ，触发流控，延迟 50ms 拉取消息
        if (cachedMessageSizeInMiB > this.defaultMQPushConsumer.getPullThresholdSizeForQueue()) {
            this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
            if ((queueFlowControlTimes++ % 1000) == 0) {
                log.warn(
                    "the cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, pullRequest={}, flowControlTimes={}",
                    this.defaultMQPushConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, pullRequest, queueFlowControlTimes);
            }
            return;
        }

        // 并发消息
        if (!this.consumeOrderly) {
            // todo 已经从 ProcessQueue 对应的 MessageQueue 中拉取的消息的最大消息进度和最小消息进度差值达到 并发消费时-消息处理队列最大跨度(默认 2000) 触发流控，延迟 50ms;
            //  主要是针对消费进度(逻辑偏移量)小的消息迟迟消费不了的情况，也就是越早发送的消息，越晚被消费，那么就会造成消息堆积，触发流控，延迟 50ms;
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL);
                if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                    log.warn(
                        "the queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, pullRequest={}, flowControlTimes={}",
                        processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(),
                        pullRequest, queueMaxSpanFlowControlTimes);
                }
                return;
            }
        // 有序消费
        } else {
            // todo 消息处理队列必须处于锁定状态(当前消费者持有消息处理队列所在 Broker 上的分布式锁，才能保证消息处理队列的消费顺序)
            if (processQueue.isLocked()) {

                // 在拉取消息时，保证 PullRequest 的初始拉取点，只在第一次拉取时设置
                if (!pullRequest.isPreviouslyLocked()) {
                    long offset = -1L;
                    try {
                        // 计算消息队列开始的消费进度
                        offset = this.rebalanceImpl.computePullFromWhereWithException(pullRequest.getMessageQueue());
                    } catch (MQClientException e) {
                        this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                        log.error("Failed to compute pull offset, pullResult: {}", pullRequest, e);
                        return;
                    }

                    // 如果获取的 Offset(逻辑偏移量) 小于拉取消息请求的 Offset(逻辑偏移量)，说明要拉取消息请求的进度有问题，需要修正拉取消息请求的逻辑偏移量
                    boolean brokerBusy = offset < pullRequest.getNextOffset();
                    // 默认消费方式 CONSUME_FROM_LAST_OFFSET(从上次偏移量开始消费)
                    log.info("the first time to pull message, so fix offset from broker. pullRequest: {} NewOffset: {} brokerBusy: {}",
                        pullRequest, offset, brokerBusy);
                    if (brokerBusy) {
                        log.info("[NOTIFYME]the first time to pull message, but pull request offset larger than broker consume offset. pullRequest: {} NewOffset: {}",
                            pullRequest, offset);
                    }

                    // 保证 PullRequest 的初始拉取点在拉取时，只在第一次拉取时设置
                    pullRequest.setPreviouslyLocked(true);

                    // 修正消息拉取点
                    pullRequest.setNextOffset(offset);
                }

              // 消息处理队列，没有被锁定，延迟拉取消息 3000ms
            } else {
                this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
                log.info("pull message later because not locked in broker, {}", pullRequest);
                return;
            }
        }

        // todo 获取 Topic 对应的订阅信息，若不存在，则延迟 3000ms 拉取消息
        final SubscriptionData subscriptionData = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (null == subscriptionData) {
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            log.warn("find the consumer's subscription failed, {}", pullRequest);
            return;
        }

        final long beginTimestamp = System.currentTimeMillis();

        // 拉取消息回调
        // todo 不断通过向 PullRequestQueue 中加入拉取消息的请求
        PullCallback pullCallback = new PullCallback() {
            /**
             * 拉取消息成功
             * @param pullResult
             */
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {

                    // 处理拉取消息的结果，即提取 ByteBuffer 生成的 List<MessageExt> 并使用 Tag 过滤消息(此处是在 Broker 过滤消息)
                    // 注意：在拉取的时候，指定了哪个队列、从哪个位置拉取、拉取的消息数量
                    // todo 消费方过滤消息
                    pullResult = DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(pullRequest.getMessageQueue(), pullResult,
                        subscriptionData);

                    // 根据拉取状态结果，分别处理
                    // todo 就算有消息过滤，那么消费位置也会不断向前推进，不会造成积压
                    //  即 RocketMQ NG 的 diffTotal 的计算逻辑是 BrokerOffset - ConsumerOffset ，不会造成大量积压，也会推进
                    switch (pullResult.getPullStatus()) {

                        // 1. 找到消息直接将这一批(默认最多一次拉取 32 条)，先丢到 ProcessQueue 中，然后直接将这批消息 submit 到 ConsumerMessageService 的线程池
                        // 在 submitConsumerRequest 会根据 consumerMessageBatchSize 分批提交给消费线程去消费消息，ConsumerMessageBatchSize 默认为 1
                        case FOUND:
                            // 临时保存当前拉取消息是从哪个位置拉取的
                            long prevRequestOffset = pullRequest.getNextOffset();
                            // 更新下次拉取请求的逻辑偏移量(类似下标)，复用拉取消息请求 PullRequest
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            // 统计消息消费的响应时间
                            long pullRT = System.currentTimeMillis() - beginTimestamp;
                            DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullRT(pullRequest.getConsumerGroup(),
                                pullRequest.getMessageQueue().getTopic(), pullRT);

                            /**
                             * 如果最终没有获取到消息，则立即将 PullRequest 放入 PullMessageService#PullRequestQueue 队列中，以便及时再次执行消息拉取
                             * todo 为什么会存在拉取到消息，但是消息结果为空呢? 原因在 pullAPIWrapper#processPullResult() 中，根本原因是在于消息根据 Tag 进行了过滤
                             * 1) 在服务端只验证了 Tag 的哈希码
                             * 2) 拉取到的消息在消费客户端中再次对消息进行过滤，这可能会出现 msgFoundList 为空
                             */
                            long firstMsgOffset = Long.MAX_VALUE;
                            if (pullResult.getMsgFoundList() == null || pullResult.getMsgFoundList().isEmpty()) {
                                DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);

                                // 有符合条件的消息
                            } else {
                                // 第一条消息在消息队列中的偏移量
                                firstMsgOffset = pullResult.getMsgFoundList().get(0).getQueueOffset();

                                // 统计
                                DefaultMQPushConsumerImpl.this.getConsumerStatsManager().incPullTPS(pullRequest.getConsumerGroup(),
                                    pullRequest.getMessageQueue().getTopic(), pullResult.getMsgFoundList().size());

                                // todo 将拉取到的消息放入到 ProcessQueue.msgTreeMap 中，作为快照：
                                //  并发消费服务中的消费请求任务，是直接处理拉取到的消息集合，后续通过这个快照处理消息消费情况(每次“消费完”，都从快照中移除)
                                //  有序消费服务中的消费请求任务，不是直接处理拉取到消息集合，而是从 ProcessQueue.consumingMsgOrderlyTreeMap(msgTreeMap的子集) 取消息并消费的
                                boolean dispatchToConsume = processQueue.putMessage(pullResult.getMsgFoundList());

                                // todo 提交消费请求到消费方(ConsumeMessageService)的线程池中，这里可能提交到并发消费服务或者有序服务中
                                //  并发消费服务：ConsumeMessageConcurrentlyService
                                //  顺序消费服务：ConsumeMessageOrderlyService
                                DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                                    pullResult.getMsgFoundList(),
                                    processQueue,
                                    pullRequest.getMessageQueue(),
                                    dispatchToConsume);

                                // 准备下次拉取消息请求
                                // todo 根据拉取频率 pullInterval，决定是就绪，立即拉取还是延迟拉取消息请求
                                if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                                    DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                        DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());

                                    // 立即拉取
                                } else {
                                    DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                                }
                            }

                            // 下次拉取消息请求的位置 小于 上次拉取消息请求的位置 || 第一条消息在消息队列中的偏移量 小于 上次拉取消息请求的位置
                            // 则判定为 BUG，输出警告日志
                            if (pullResult.getNextBeginOffset() < prevRequestOffset
                                || firstMsgOffset < prevRequestOffset) {
                                log.warn(
                                    "[BUG] pull message result maybe data wrong, nextBeginOffset: {} firstMsgOffset: {} prevRequestOffset: {}",
                                    pullResult.getNextBeginOffset(),
                                    firstMsgOffset,
                                    prevRequestOffset);
                            }

                            // 消费进度的处理交由消费程序(ConsumeMessageService)处理
                            break;

                        // 2.没有找到新的消息或没有匹配的消息，继续下个待拉取偏移量的消息拉取
                        case NO_NEW_MSG:
                        case NO_MATCHED_MSG:
                            // 设置下次拉取消息时消息队列的偏移量
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            // 修正拉取消息请求中对应的 MessageQueue 的消费进度
                            DefaultMQPushConsumerImpl.this.correctTagsOffset(pullRequest);

                            // 将拉取消息请求放入队列(pullRequestQueue)，后续进入拉取消息流程
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                            break;

                        // todo 3.偏移量非法(对应服务端的响应状态为 PULL_OFFSET_MOVED)
                        //   3.1 将 ProcessQueue 的 dropped 设置为 true，表示丢弃该消费队列，意味着 ProcessQueue 中拉取的消息将停止消费
                        //   3.2 根据服务端返回的下一次拉取偏移量尝试更新消息消费进度(内存中)，然后尝试持久化消息消费进度
                        //   3.3 将该消息队列从 RebalanceImpl 的 ProcessQueueTable 中移除，意味着暂停该消息队列中的消息拉取，等待下一次消息队列的重新负载，继续拉取消息
                        case OFFSET_ILLEGAL:
                            log.warn("the pull request offset illegal, {} {}",
                                pullRequest.toString(), pullResult.toString());

                            // 设置下次拉取消息队列位置
                            pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                            // 设置消息处理队列的 dropped 为 true
                            pullRequest.getProcessQueue().setDropped(true);

                            // 提交延迟任务，进行消息处理队列的移除，不立即移除的原因：可能有地方在使用，避免受到影响
                            DefaultMQPushConsumerImpl.this.executeTaskLater(new Runnable() {

                                @Override
                                public void run() {
                                    try {
                                        // 更新消费进度，集群消费同步到 Broker；广播消费同步到 Consumer
                                        DefaultMQPushConsumerImpl.this.offsetStore.updateOffset(pullRequest.getMessageQueue(),
                                            pullRequest.getNextOffset(), false);
                                        // 持久化消费进度
                                        DefaultMQPushConsumerImpl.this.offsetStore.persist(pullRequest.getMessageQueue());

                                        // 移除消息队列并持久化消费进度
                                        // todo 如果是有序消费，那么还需要解除当前消费者对该消息队列的锁定(解除分布式锁)
                                        DefaultMQPushConsumerImpl.this.rebalanceImpl.removeProcessQueue(pullRequest.getMessageQueue());

                                        log.warn("fix the pull request offset, {}", pullRequest);
                                    } catch (Throwable e) {
                                        log.error("executeTaskLater Exception", e);
                                    }
                                }
                            }, 10000);
                            break;
                        default:
                            break;
                    }
                }
            }

            /**
             * 拉取消息异常
             * @param e
             */
            @Override
            public void onException(Throwable e) {
                if (!pullRequest.getMessageQueue().getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    log.warn("execute the pull request exception", e);
                }

                // 准备下次拉取消息任务
                // 延迟提交拉取消息请求
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
            }
        };

        // todo 集群消费模式下，在拉取消息时，是否要将消费进度提交到 Broker ，只要消费进度 > 0 就尝试提交
        boolean commitOffsetEnable = false;
        long commitOffsetValue = 0L;
        if (MessageModel.CLUSTERING == this.defaultMQPushConsumer.getMessageModel()) {
            // 需要提交到 Broker 的消费进度
            commitOffsetValue = this.offsetStore.readOffset(pullRequest.getMessageQueue(), ReadOffsetType.READ_FROM_MEMORY);
            if (commitOffsetValue > 0) {
                // 只要本地缓存的消费进度 > 0 就需要提交消费进度到 Broker
                commitOffsetEnable = true;
            }
        }

        // todo 计算请求的订阅表达式 和 是否进行类过滤消息
        String subExpression = null;
        boolean classFilter = false;

        // 根据 MessageQueue 对应的 Topic 获取Topic对应的订阅信息
        SubscriptionData sd = this.rebalanceImpl.getSubscriptionInner().get(pullRequest.getMessageQueue().getTopic());
        if (sd != null) {
            if (this.defaultMQPushConsumer.isPostSubscriptionWhenPull() && !sd.isClassFilterMode()) {
                subExpression = sd.getSubString();
            }

            // 是否开启类过滤模式
            classFilter = sd.isClassFilterMode();
        }

        // todo 设置拉取消息时的系统标识
        // 1. 是否要提交消费进度
        // 2. 是否支持没有消息挂起请求
        // 3. 是否进行订阅表达式过滤
        // 4. 是否进行类过滤
        int sysFlag = PullSysFlag.buildSysFlag(
            commitOffsetEnable, // commitOffset
            true, // suspend
            subExpression != null, // subscription
            classFilter // class filter
        );

        // 执行从 Broker 从拉取给定消息队列的消息，如果拉取请求异常，则提交延迟拉取消息的请求
        try {
            // todo 异步执行消息拉取请求，注意：这里是异步，拉取后将结果传递给 PullCallback 的回调中
            this.pullAPIWrapper.pullKernelImpl(
                pullRequest.getMessageQueue(), // 从该消息队列拉取消息
                subExpression, // 订阅表示
                subscriptionData.getExpressionType(), // 过滤类型，默认 Tag
                subscriptionData.getSubVersion(), // 订阅信息版本
                pullRequest.getNextOffset(), // 本次拉取消息的逻辑偏移量(ConsumeQueue 的逻辑偏移量)
                this.defaultMQPushConsumer.getPullBatchSize(),// Consumer 从 Broker 拉取的待消费消息的数量，默认是 32 [注意：从 Broker 拉取的时候，不一定是 32 条，原因多种 具体看 Broker]
                sysFlag,// 系统标识
                commitOffsetValue,// 要提交到 Broker 的消费进度
                BROKER_SUSPEND_MAX_TIME_MILLIS, // Broker 挂起请求的最大时间 默认 15s
                CONSUMER_TIMEOUT_MILLIS_WHEN_SUSPEND, // 拉取消息超时时间 默认 30s
                CommunicationMode.ASYNC, // 异步拉取消息
                pullCallback // 拉取消息后的回调函数
            );
        } catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            // 执行拉取消息请求异常，延迟拉取消息请求
            this.executePullRequestLater(pullRequest, pullTimeDelayMillsWhenException);
        }
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }

    public boolean isPause() {
        return pause;
    }

    public void setPause(boolean pause) {
        this.pause = pause;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return this.mQClientFactory.getConsumerStatsManager();
    }

    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    /**
     * 修正消费进度，适用于拉取请求的响应结果状态是：没有找到新的消息 || 没有匹配的消息
     *
     * @param pullRequest
     */
    private void correctTagsOffset(final PullRequest pullRequest) {
        if (0L == pullRequest.getProcessQueue().getMsgCount().get()) {
            this.offsetStore.updateOffset(pullRequest.getMessageQueue(), pullRequest.getNextOffset(), true);
        }
    }

    public void executeTaskLater(final Runnable r, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executeTaskLater(r, timeDelay);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey) throws MQClientException,
        InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * 注册消费者监听器(其实是消费者拉取到消息后，进行的具体业务实现)
     * @param messageListener
     */
    public void registerMessageListener(MessageListener messageListener) {
        this.messageListenerInner = messageListener;
    }

    public void resume() {
        this.pause = false;
        doRebalance();
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    /**
     * 发回消息失败消息
     *
     * @param msg        消息
     * @param delayLevel 延迟级别
     * @param brokerName Broker 名称
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    public void sendMessageBack(MessageExt msg, int delayLevel, final String brokerName)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            // 获取主 Broker 信息
            String brokerAddr = (null != brokerName) ? this.mQClientFactory.findBrokerAddressInPublish(brokerName)
                : RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());

            // Consumer 发回消息
            // todo  broker 端可以自动创建重试主题
            this.mQClientFactory.getMQClientAPIImpl().consumerSendMessageBack(brokerAddr, msg,
                this.defaultMQPushConsumer.getConsumerGroup(), delayLevel, 5000, getMaxReconsumeTimes());

            // 发生异常时，Consumer 使用内置默认 Producer 发送消息
        } catch (Exception e) {
            // todo 消息消费失败，添加到该 Topic 的重试队列中
            log.error("sendMessageBack Exception, " + this.defaultMQPushConsumer.getConsumerGroup(), e);

            // 基于原来消息 Tag 构建新的消息，设置新的主题，即重试主题
            // todo broker 端可以自动创建重试主题，即使没有开启自动创建主题的情况下
            Message newMsg = new Message(MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup()), msg.getBody());

            // 原有消息Id
            String originMsgId = MessageAccessor.getOriginMessageId(msg);
            MessageAccessor.setOriginMessageId(newMsg, UtilAll.isBlank(originMsgId) ? msg.getMsgId() : originMsgId);

            newMsg.setFlag(msg.getFlag());
            MessageAccessor.setProperties(newMsg, msg.getProperties());

            // 保留原有的 Topic 到 RETRY_TOPIC 属性中
            MessageAccessor.putProperty(newMsg, MessageConst.PROPERTY_RETRY_TOPIC, msg.getTopic());
            // 设置重试次数到 RECONSUME_TIMES 属性中
            MessageAccessor.setReconsumeTime(newMsg, String.valueOf(msg.getReconsumeTimes() + 1));
            // 设置系统允许的最大重试次数
            MessageAccessor.setMaxReconsumeTimes(newMsg, String.valueOf(getMaxReconsumeTimes()));
            MessageAccessor.clearProperty(newMsg, MessageConst.PROPERTY_TRANSACTION_PREPARED);

            // 设置延迟级别，基于重试次数
            newMsg.setDelayTimeLevel(3 + msg.getReconsumeTimes());

            this.mQClientFactory.getDefaultMQProducer().send(newMsg);
        } finally {
            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
        }
    }

    /**
     * 获取消息消费最大重试次数 16
     * @return
     */
    private int getMaxReconsumeTimes() {
        // default reconsume times: 16
        if (this.defaultMQPushConsumer.getMaxReconsumeTimes() == -1) {
            return 16;

            // 非默认的情况
        } else {
            return this.defaultMQPushConsumer.getMaxReconsumeTimes();
        }
    }

    public void shutdown() {
        shutdown(0);
    }

    public synchronized void shutdown(long awaitTerminateMillis) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.consumeMessageService.shutdown(awaitTerminateMillis);
                this.persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
                this.mQClientFactory.shutdown();
                log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
                this.rebalanceImpl.destroy();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    /**
     * todo 消费者启动核心代码
     * 消费者启动，开始消费消息
     *
     * @throws MQClientException
     */
    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                //todo 刚刚创建
                log.info("the consumer [{}] start beginning. messageModel={}, isUnitMode={}", this.defaultMQPushConsumer.getConsumerGroup(),
                    this.defaultMQPushConsumer.getMessageModel(), this.defaultMQPushConsumer.isUnitMode());
                // 并不代表启动失败
                this.serviceState = ServiceState.START_FAILED;

                // TODO: 0.1.校验消费者配置信息，其实就是校验消费者是否合法
                this.checkConfig();


                // TODO: 0.2
                //  1. 构建 Topic 的订阅信息(SubscriptionData)，并添加到 rebalanceImpl 的订阅信息中
                //  2. 集群消费模式下，会基于消费者所在的组自动订阅 Topic 的重试主题
                this.copySubscription();

                // 如果是集群消费，设置 instanceName 为一个字符串化的数字，比如 10072
                if (this.defaultMQPushConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultMQPushConsumer.changeInstanceNameToPID();
                }

                // TODO: 0.3.创建 MQClientInstance 实例，ClientId 为 Ip@instanceName@unitName(可选)
                // TODO: 这个实例在同一个JVM中消费者和生产者共用，在 MQClientManager 维护了一个 factoryTable，用来存储 MQClientInstance
                // TODO: factoryTable 实际是一个 ConcurrentHashMap<clientId,MQClientInstance>
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQPushConsumer, this.rpcHook);

                // TODO: 1.1 丰富 rebalanceImpl 对象属性，完成后，便具备了负载均衡的能力
                // 设置消费组名，为指定的消费组内的消费者分配消息队列
                this.rebalanceImpl.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());

                // 消息模式：默认为集群消费
                // 集群模式：每条消息被同一个消费组内的其中一个消费者消费一次即可
                // 广播模式：每条消息都被同一个消费组内的所有消费者都消费一次
                this.rebalanceImpl.setMessageModel(this.defaultMQPushConsumer.getMessageModel());

                // todo 设置为同一个消费组内的消费者分配消息队列的策略
                this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultMQPushConsumer.getAllocateMessageQueueStrategy());

                // 设置客户端实例
                this.rebalanceImpl.setmQClientFactory(this.mQClientFactory);


                // TODO 5 拉取消息（无论是拉模式，还是推模式：都是拉取消息）
                // TODO PullAPIWrapper 拉取消息的 api包装类，主要有消息的拉取和接受拉取到的消息
                this.pullAPIWrapper = new PullAPIWrapper(mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup(), isUnitMode());
                this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);

                // todo 6 消费进度处理器
                // 集群模式：消费进度保存在 Broker 上，因为同一组内的消费者之间要共享进度
                // 广播模式：消费进度保存在消费者端
                if (this.defaultMQPushConsumer.getOffsetStore() != null) {
                    this.offsetStore = this.defaultMQPushConsumer.getOffsetStore();
                } else {
                    // 根据消费模式，创建对应的 OffsetStore 对象
                    switch (this.defaultMQPushConsumer.getMessageModel()) {
                        case BROADCASTING:
                            // 广播模式，消费进度根据消费者保存在一起，即消费者本地
                            this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        case CLUSTERING:
                            // 集群模式：消费进度需要集中保存，适合在 Broker 端保存
                            this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultMQPushConsumer.getConsumerGroup());
                            break;
                        default:
                            break;
                    }
                    // 回填外部的 offsetStore
                    this.defaultMQPushConsumer.setOffsetStore(this.offsetStore);
                }
                // 集群模式下，不需要加载，消息消费进度存储在 Broker，从 Broker 上获取即可
                // 广播模式下，需要加载，消息消费进度存储在消费者本地，
                this.offsetStore.load();


                // todo 7. 根据消息监听器类型，创建不同的消息消费服务，并启动消费服务
                // 有序消费服务
                if (this.getMessageListenerInner() instanceof MessageListenerOrderly) {
                    this.consumeOrderly = true;
                    this.consumeMessageService =
                        new ConsumeMessageOrderlyService(this, (MessageListenerOrderly) this.getMessageListenerInner());

                // 并发消费服务
                } else if (this.getMessageListenerInner() instanceof MessageListenerConcurrently) {
                    this.consumeOrderly = false;
                    this.consumeMessageService =
                        new ConsumeMessageConcurrentlyService(this, (MessageListenerConcurrently) this.getMessageListenerInner());
                }
                // 并发消费 && (集群模式 || 广播模式)，都会启动清理过期消息的定时任务
                // 有序消费 && 集群模式，会启动每隔一段时间锁定一次消费者分配到消息队列的定时任务
                this.consumeMessageService.start();

                // todo 8. 向客户端实例中设置消费者
                boolean registerOK = mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    this.consumeMessageService.shutdown(defaultMQPushConsumer.getAwaitTerminationMillisWhenShutdown());
                    throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }


                // TODO: 9 重要：第三步获取的 MQClientInstance 启动
                mQClientFactory.start();
                log.info("the consumer [{}] start OK.", this.defaultMQPushConsumer.getConsumerGroup());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PushConsumer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }
        // todo 10 基于订阅信息，更新Topic路由信息到本地
        this.updateTopicSubscribeInfoWhenSubscriptionChanged();

        // todo 11 校验客户端在 Broker 的状态
        this.mQClientFactory.checkClientInBroker();

        // todo 12 Consumer 启动成功，立即发送心跳到所有的 Broker
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        // todo 13 唤醒均衡消息队列任务(RebalanceService) 负责当前 Consumer 可消费的消息队列，即 Consumer 上线会立即触发一次消息队列的负载均衡
        this.mQClientFactory.rebalanceImmediately();
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQPushConsumer.getConsumerGroup());
        // todo 消费者所属的group不能为空
        if (null == this.defaultMQPushConsumer.getConsumerGroup()) {
            throw new MQClientException(
                "consumerGroup is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 消费者所属的group不能是默认的消息者group (DEFAULT_CONSUMER)
        if (this.defaultMQPushConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                "consumerGroup can not equal "
                    + MixAll.DEFAULT_CONSUMER_GROUP
                    + ", please specify another one."
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 消费者的消费模式不能为空，默认集群消费
        if (null == this.defaultMQPushConsumer.getMessageModel()) {
            throw new MQClientException(
                "messageModel is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 消费者从哪个地方开始消费，默认从上次消费停止的地方开始消费
        // todo 这里的上次消费停止的地方，分为两种情况：
        // todo 1. 如果消费者创建的过晚，最开始订阅的消息，还未过期，则从头开始消费
        // todo 2. 最开始订阅的消息已经过期，则从最新的消息开始消费，但是消费者启动时，引导时间戳之前的消息将不再被消费
        // todo 消费者从哪个地方开始消费
        if (null == this.defaultMQPushConsumer.getConsumeFromWhere()) {
            throw new MQClientException(
                "consumeFromWhere is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 消费者启动引导时间戳
        Date dt = UtilAll.parseDate(this.defaultMQPushConsumer.getConsumeTimestamp(), UtilAll.YYYYMMDDHHMMSS);
        if (null == dt) {
            throw new MQClientException(
                "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received "
                    + this.defaultMQPushConsumer.getConsumeTimestamp()
                    + " " + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL), null);
        }

        // allocateMessageQueueStrategy
        // 消息队列分配策略不能为空，默认平均分配策略
        if (null == this.defaultMQPushConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                "allocateMessageQueueStrategy is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // subscription
        // 消费者订阅信息，在启动时该 Map 里面没有数据，订阅信息被设置到了消费者内部类的 Rebalance 中
        if (null == this.defaultMQPushConsumer.getSubscription()) {
            throw new MQClientException(
                "subscription is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }

        // messageListener
        if (null == this.defaultMQPushConsumer.getMessageListener()) {
            throw new MQClientException(
                "messageListener is null"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 消费者是有序消费还是同步消费，根据消息监听器确定
        boolean orderly = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly;
        boolean concurrently = this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently;
        if (!orderly && !concurrently) {
            throw new MQClientException(
                "messageListener must be instanceof MessageListenerOrderly or MessageListenerConcurrently"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 消费者最小消费线程数是否合规，默认 20
        // consumeThreadMin
        if (this.defaultMQPushConsumer.getConsumeThreadMin() < 1
            || this.defaultMQPushConsumer.getConsumeThreadMin() > 1000) {
            throw new MQClientException(
                "consumeThreadMin Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 消费者最大消费线程数是否合规，默认 20
        // consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMax() < 1 || this.defaultMQPushConsumer.getConsumeThreadMax() > 1000) {
            throw new MQClientException(
                "consumeThreadMax Out of range [1, 1000]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 消费者最小消费线程数不能大于最大消费线程数
        // consumeThreadMin can't be larger than consumeThreadMax
        if (this.defaultMQPushConsumer.getConsumeThreadMin() > this.defaultMQPushConsumer.getConsumeThreadMax()) {
            throw new MQClientException(
                "consumeThreadMin (" + this.defaultMQPushConsumer.getConsumeThreadMin() + ") "
                    + "is larger than consumeThreadMax (" + this.defaultMQPushConsumer.getConsumeThreadMax() + ")",
                null);
        }
        // todo 并发消费下，单条consume queue队列允许的最大offset跨度，达到则触发流控，默认 2000
        // consumeConcurrentlyMaxSpan
        if (this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() < 1
            || this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan() > 65535) {
            throw new MQClientException(
                "consumeConcurrentlyMaxSpan Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo consume queue流控的阈值 默认 1000
        // pullThresholdForQueue
        if (this.defaultMQPushConsumer.getPullThresholdForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdForQueue() > 65535) {
            throw new MQClientException(
                "pullThresholdForQueue Out of range [1, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 主题级别的流控制阈值 默认 -1
        // pullThresholdForTopic
        if (this.defaultMQPushConsumer.getPullThresholdForTopic() != -1) {
            if (this.defaultMQPushConsumer.getPullThresholdForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdForTopic() > 6553500) {
                throw new MQClientException(
                    "pullThresholdForTopic Out of range [1, 6553500]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }
        // todo 队列级别上限制缓存的消息大小 默认 100MB
        // pullThresholdSizeForQueue
        if (this.defaultMQPushConsumer.getPullThresholdSizeForQueue() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForQueue() > 1024) {
            throw new MQClientException(
                "pullThresholdSizeForQueue Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 限制主题级别的缓存消息大小 默认 -1
        if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() != -1) {
            // pullThresholdSizeForTopic
            if (this.defaultMQPushConsumer.getPullThresholdSizeForTopic() < 1 || this.defaultMQPushConsumer.getPullThresholdSizeForTopic() > 102400) {
                throw new MQClientException(
                    "pullThresholdSizeForTopic Out of range [1, 102400]"
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
            }
        }
        // todo 拉取的间隔 默认 0
        // pullInterval
        if (this.defaultMQPushConsumer.getPullInterval() < 0 || this.defaultMQPushConsumer.getPullInterval() > 65535) {
            throw new MQClientException(
                "pullInterval Out of range [0, 65535]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 批量消息消费的最大条数 默认 1
        // consumeMessageBatchMaxSize
        if (this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() < 1
            || this.defaultMQPushConsumer.getConsumeMessageBatchMaxSize() > 1024) {
            throw new MQClientException(
                "consumeMessageBatchMaxSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
        // todo 一次最大拉取的批量大小 默认 32 条
        // pullBatchSize
        if (this.defaultMQPushConsumer.getPullBatchSize() < 1 || this.defaultMQPushConsumer.getPullBatchSize() > 1024) {
            throw new MQClientException(
                "pullBatchSize Out of range [1, 1024]"
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                null);
        }
    }

    /**
     * 根据消费者的订阅关系，创建订阅对象
     * todo 重要：集群消费时，会加入当前消费组重试消息的订阅
     * @throws MQClientException
     */
    private void copySubscription() throws MQClientException {
        try {
            // 获取消费者订阅关系
            Map<String, String> sub = this.defaultMQPushConsumer.getSubscription();
            if (sub != null) {
                for (final Map.Entry<String, String> entry : sub.entrySet()) {
                    final String topic = entry.getKey();
                    final String subString = entry.getValue();

                    // 构建订阅数据，并缓存到消费者的 rebalanceImpl 负载均衡器中
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subString);
                    this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
                }
            }

            if (null == this.messageListenerInner) {
                this.messageListenerInner = this.defaultMQPushConsumer.getMessageListener();
            }

            // 如果是消费模式是集群消费，需要订阅重试主题
            switch (this.defaultMQPushConsumer.getMessageModel()) {
                case BROADCASTING:
                    break;
                case CLUSTERING:
                    // todo 集群消费模式下，给该消费者订阅一个 "%RETRY%"+消费者group的 topic （重试主题）
                    //  注意：重试主题的订阅表示为 *
                    final String retryTopic = MixAll.getRetryTopic(this.defaultMQPushConsumer.getConsumerGroup());
                    SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(retryTopic, SubscriptionData.SUB_ALL);
                    this.rebalanceImpl.getSubscriptionInner().put(retryTopic, subscriptionData);
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public MessageListener getMessageListenerInner() {
        return messageListenerInner;
    }

    /**
     * 基于订阅信息，更新当前 JVM 实例中的客户端(生产者或消费者)的路由信息
     */
    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionInner() {
        return this.rebalanceImpl.getSubscriptionInner();
    }

    /**
     * 消费者订阅 Topic
     *
     * @param topic Topic
     * @param subExpression 订阅表达式
     * @throws MQClientException
     */
    public void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            // 根据 Topic、订阅表达式构建订阅数据
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, subExpression);

            // 存储 Topic 的订阅数据
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);

            // todo 通过心跳同步 Consumer 和 Producer 信息到 Broker
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(String topic, String fullClassName, String filterClassSource) throws MQClientException {
        try {
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(topic, "*");
            subscriptionData.setSubString(fullClassName);
            subscriptionData.setClassFilterMode(true);
            subscriptionData.setFilterClassSource(filterClassSource);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }

        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void subscribe(final String topic, final MessageSelector messageSelector) throws MQClientException {
        try {
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }

            SubscriptionData subscriptionData = FilterAPI.build(topic,
                messageSelector.getExpression(), messageSelector.getExpressionType());

            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            if (this.mQClientFactory != null) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
            }
        } catch (Exception e) {
            throw new MQClientException("subscription exception", e);
        }
    }

    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }

    public void unsubscribe(String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
    }

    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset, false);
    }

    public void updateCorePoolSize(int corePoolSize) {
        this.consumeMessageService.updateCorePoolSize(corePoolSize);
    }

    public MessageExt viewMessage(String msgId)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public boolean isConsumeOrderly() {
        return consumeOrderly;
    }

    public void setConsumeOrderly(boolean consumeOrderly) {
        this.consumeOrderly = consumeOrderly;
    }

    public void resetOffsetByTimeStamp(long timeStamp)
        throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        for (String topic : rebalanceImpl.getSubscriptionInner().keySet()) {
            Set<MessageQueue> mqs = rebalanceImpl.getTopicSubscribeInfoTable().get(topic);
            Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
            if (mqs != null) {
                for (MessageQueue mq : mqs) {
                    long offset = searchOffset(mq, timeStamp);
                    offsetTable.put(mq, offset);
                }
                this.mQClientFactory.resetOffset(topic, groupName(), offsetTable);
            }
        }
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    @Override
    public String groupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return this.defaultMQPushConsumer.getConsumeFromWhere();
    }

    @Override
    public Set<SubscriptionData> subscriptions() {
        Set<SubscriptionData> subSet = new HashSet<SubscriptionData>();

        subSet.addAll(this.rebalanceImpl.getSubscriptionInner().values());

        return subSet;
    }

    /**
     * 执行消息队列分配
     */
    @Override
    public void doRebalance() {
        if (!this.pause) {
            // todo 12.3.4 负载均衡(是否是有序消费)，调用消费者中的 RebalanceImpl
            this.rebalanceImpl.doRebalance(this.isConsumeOrderly());
        }
    }

    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
            mqs.addAll(allocateMq);

            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup() + " persistConsumerOffset exception", e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQPushConsumer.isUnitMode();
    }

    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultMQPushConsumer);

        prop.put(ConsumerRunningInfo.PROP_CONSUME_ORDERLY, String.valueOf(this.consumeOrderly));
        prop.put(ConsumerRunningInfo.PROP_THREADPOOL_CORE_SIZE, String.valueOf(this.consumeMessageService.getCorePoolSize()));
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));

        info.setProperties(prop);

        Set<SubscriptionData> subSet = this.subscriptions();
        info.getSubscriptionSet().addAll(subSet);

        Iterator<Entry<MessageQueue, ProcessQueue>> it = this.rebalanceImpl.getProcessQueueTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            MessageQueue mq = next.getKey();
            ProcessQueue pq = next.getValue();

            ProcessQueueInfo pqinfo = new ProcessQueueInfo();
            pqinfo.setCommitOffset(this.offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE));
            pq.fillProcessQueueInfo(pqinfo);
            info.getMqTable().put(mq, pqinfo);
        }

        for (SubscriptionData sd : subSet) {
            ConsumeStatus consumeStatus = this.mQClientFactory.getConsumerStatsManager().consumeStatus(this.groupName(), sd.getTopic());
            info.getStatusTable().put(sd.getTopic(), consumeStatus);
        }

        return info;
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    public void setmQClientFactory(MQClientInstance mQClientFactory) {
        this.mQClientFactory = mQClientFactory;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    //Don't use this deprecated setter, which will be removed soon.
    @Deprecated
    public synchronized void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public void adjustThreadPool() {
        long computeAccTotal = this.computeAccumulationTotal();
        long adjustThreadPoolNumsThreshold = this.defaultMQPushConsumer.getAdjustThreadPoolNumsThreshold();

        long incThreshold = (long) (adjustThreadPoolNumsThreshold * 1.0);

        long decThreshold = (long) (adjustThreadPoolNumsThreshold * 0.8);

        if (computeAccTotal >= incThreshold) {
            this.consumeMessageService.incCorePoolSize();
        }

        if (computeAccTotal < decThreshold) {
            this.consumeMessageService.decCorePoolSize();
        }
    }

    private long computeAccumulationTotal() {
        long msgAccTotal = 0;
        ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = this.rebalanceImpl.getProcessQueueTable();
        Iterator<Entry<MessageQueue, ProcessQueue>> it = processQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, ProcessQueue> next = it.next();
            ProcessQueue value = next.getValue();
            msgAccTotal += value.getMsgAccCnt();
        }

        return msgAccTotal;
    }

    public List<QueueTimeSpan> queryConsumeTimeSpan(final String topic)
        throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        List<QueueTimeSpan> queueTimeSpan = new ArrayList<QueueTimeSpan>();
        TopicRouteData routeData = this.mQClientFactory.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic, 3000);
        for (BrokerData brokerData : routeData.getBrokerDatas()) {
            String addr = brokerData.selectBrokerAddr();
            queueTimeSpan.addAll(this.mQClientFactory.getMQClientAPIImpl().queryConsumeTimeSpan(addr, topic, groupName(), 3000));
        }

        return queueTimeSpan;
    }

    /**
     * 处理重试消息，还原对应的 Topic
     * @param msgs
     * @param consumerGroup
     */
    public void resetRetryAndNamespace(final List<MessageExt> msgs, String consumerGroup) {
        // 重试 topic , %RETRY% + consumerGroup
        final String groupTopic = MixAll.getRetryTopic(consumerGroup);
        for (MessageExt msg : msgs) {
            String retryTopic = msg.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
            // 如果是重试队列，则还原真实的 Topic
            if (retryTopic != null && groupTopic.equals(msg.getTopic())) {
                msg.setTopic(retryTopic);
            }

            if (StringUtils.isNotEmpty(this.defaultMQPushConsumer.getNamespace())) {
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQPushConsumer.getNamespace()));
            }
        }
    }

    public ConsumeMessageService getConsumeMessageService() {
        return consumeMessageService;
    }

    public void setConsumeMessageService(ConsumeMessageService consumeMessageService) {
        this.consumeMessageService = consumeMessageService;

    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }
}
