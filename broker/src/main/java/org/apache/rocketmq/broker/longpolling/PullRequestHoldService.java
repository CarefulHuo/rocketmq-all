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
package org.apache.rocketmq.broker.longpolling;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.ConsumeQueueExt;

/**
 * PullRequestHoldService的类，继承自ServiceThread类。它的主要作用是管理拉取请求（PullRequest），在消息队列RocketMQ中起到等待和通知消息到达的作用。
 *
 */
public class PullRequestHoldService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 消息主题和消息队列Id的分隔符
     */
    private static final String TOPIC_QUEUEID_SEPARATOR = "@";

    /**
     * BrokerController 作为构造参数，用于获取BrokerController的配置信息和进行一些操作
     */
    private final BrokerController brokerController;

    /**
     * 用来获取当前系统时间
     */
    private final SystemClock systemClock = new SystemClock();

    /**
     * 存储PullRequest的容器
     * key：topic@queueId 标记哪个消费队列
     * value：ManyPullRequest(内部是一个 ArrayList 集合，包含多个 pullRequest 请求，为保证原子性，使用 synchronized 关键字加锁)
     */
    private ConcurrentMap<String/* topic@queueId */, ManyPullRequest> pullRequestTable = new ConcurrentHashMap<String, ManyPullRequest>(1024);

    public PullRequestHoldService(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 将拉取消息挂起请求添加到 pullRequestTable 中，以 Topic@queueId 为 维度，标志了是哪个消息队列
     * @param topic 消息主题
     * @param queueId 消息队列id
     * @param pullRequest 拉取消息请求
     */
    public void suspendPullRequest(final String topic, final int queueId, final PullRequest pullRequest) {
        // 主题 + 队列号，标识是哪个消费队列
        String key = this.buildKey(topic, queueId);
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        // 如果没有请求集合，则新建一个 ManyPullRequest 对象
        if (null == mpr) {
            mpr = new ManyPullRequest();
            ManyPullRequest prev = this.pullRequestTable.putIfAbsent(key, mpr);
            if (prev != null) {
                mpr = prev;
            }
        }
        // 将拉取消息请求放入到指定的消费队列的拉取请求集合中
        mpr.addPullRequest(pullRequest);
    }

    /**
     * 用于构建存储拉取请求的 key：主题 + 消费队列Id ，作为唯一标识
     * @param topic
     * @param queueId
     * @return
     */
    private String buildKey(final String topic, final int queueId) {
        StringBuilder sb = new StringBuilder();
        sb.append(topic);
        sb.append(TOPIC_QUEUEID_SEPARATOR);
        sb.append(queueId);
        return sb.toString();
    }

    /**
     * todo 通过不断检查被 hold 住的请求，检查是否已经有数据了，具体检查哪些请求：
     * 是在 org.apache.rocketmq.broker.processor.PullMessageProcessor#processRequest() 方法中，
     * Switch 分支为 ResponseCode.PULL_NOT_FOUND 里面调用 suspendPullRequest() 方法添加的请求
     * 线程的运行方法，不断循环检测是否需要等待或进行其他操作。
     */
    @Override
    public void run() {
        log.info("{} service started", this.getServiceName());
        while (!this.isStopped()) {
            try {
                // 判断是否开启长轮询
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    // 长轮询开启的话，每隔 5s 去检查是否有需要处理的 PullRequest
                    // todo 一般客户端指定的长轮询时间比这个大，默认是 15s
                    this.waitForRunning(5 * 1000);
                } else {
                    // todo 如果不开启长轮询模式，则只挂起一次，挂起时间为 BrokerConfig.shortPollingTimeMills 然后判断消息是否到达
                    // todo 因为 PullMessageProcessor#processRequest() 方法中响应结果为 ResponseCode.PULL_NOT_FOUND && 没有开启长轮询，那么设置拉取消息请求设置的挂起(等待)时间就是 1s
                    //  那么，线程等待 1s 后，就会立即唤醒线程执行拉取消息挂起请求；检查是否有消息到达，那么即使下次触发时没有消息，也会到达超时时间，这个叫短轮询
                    // @see PullRequestHoldService#notifyMessageArriving() 方法内的这段代码
                    //                  if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                    //                        try {
                    //                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                    //                                request.getRequestCommand());
                    //                        } catch (Throwable e) {
                    //                            log.error("execute request when wakeup failed.", e);
                    //                        }
                    //                        continue;
                    //                    }

                    this.waitForRunning(this.brokerController.getBrokerConfig().getShortPollingTimeMills());
                }

                long beginLockTimestamp = this.systemClock.now();
                // 检查挂起的拉取消息请求，是否有消息到达
                this.checkHoldRequest();
                long costTime = this.systemClock.now() - beginLockTimestamp;
                if (costTime > 5 * 1000) {
                    log.info("[NOTIFYME] check hold request cost {} ms.", costTime);
                }
            } catch (Throwable e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info("{} service end", this.getServiceName());
    }

    /**
     * 获取 PullRequestHoldService 的服务名称，即类名
     * @return
     */
    @Override
    public String getServiceName() {
        return PullRequestHoldService.class.getSimpleName();
    }

    /**
     * 遍历挂起的拉取消息请求，检查是否有消息到来
     * <p>
     *     原则：获取指定 MessageQueue(消息队列) 下目前最大的 offset ，然后用来和拉取任务的待拉取偏移量比较，来确定是否有新的消息到来，具体实现在  notifyMessageArriving 方法内
     *     过程：checkHoldRequest 方法属于主动检查，它是定时检查所有挂起的拉取消息请求，不区分是针对哪个消息队列的拉取消息请求
     * </p>
     */
    private void checkHoldRequest() {
        // 遍历拉取请求集合，遍历指定的 Topic 下某个 Queue 是否有消息到达
        for (String key : this.pullRequestTable.keySet()) {
            String[] kArray = key.split(TOPIC_QUEUEID_SEPARATOR);
            if (2 == kArray.length) {
                String topic = kArray[0];
                int queueId = Integer.parseInt(kArray[1]);
                // todo 根据消息主题和消息队列Id，获取消息队列中目前的最大偏移量
                final long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                try {
                    this.notifyMessageArriving(topic, queueId, offset);
                } catch (Throwable e) {
                    log.error("check hold request failed. topic={}, queueId={}", topic, queueId, e);
                }
            }
        }
    }

    /**
     * 检查是否有需要通知的请求
     * @param topic 消息主题
     * @param queueId 消息队列Id
     * @param maxOffset 消息队列目前最大的偏移量
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset) {
        notifyMessageArriving(topic, queueId, maxOffset, null, 0, null, null);
    }

    /**
     * 检查指定 Topic下的指定消息队列 是否有消息到达
     * 判断依据：todo 比较待拉取偏移量(pullFromThisOffset) 和消息队列的最大有效偏移量
     *
     * @param topic        消息主题
     * @param queueId      消息队列Id
     * @param maxOffset    消息队列目前最大的逻辑偏移量
     * @param tagsCode     tag 的 HashCode (用于基于 tag 筛选消息)或是延时消息的投递时间
     * @param msgStoreTime 消息存储的时间
     * @param filterBitMap 过滤位图
     * @param properties   消息属性
     */
    public void notifyMessageArriving(final String topic, final int queueId, final long maxOffset, final Long tagsCode,
        long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

        // 1. 标志 Topic 下 Queue ，即哪个消息队列来消息了
        String key = this.buildKey(topic, queueId);

        // 2. 获取具体 queue 对应的拉取消息请求集合
        ManyPullRequest mpr = this.pullRequestTable.get(key);
        if (mpr != null) {
            // 3. 获取主题与队列的所有 PullRequest 并清除内部 PullRequest 集合，避免重复校验
            List<PullRequest> requestList = mpr.cloneListAndClear();
            if (requestList != null) {
                // 需要重试的拉取消息请求
                List<PullRequest> replayList = new ArrayList<PullRequest>();
                // 遍历当前 queue 下每个等待拉取消息的请求
                for (PullRequest request : requestList) {
                    // 4.1 如果消息请求的待拉取偏移量(pullFromThisOffset)大于消息队列的最大有效偏移量，则再次获取消息队列的最大有效偏移量，再给一次机会
                    long newestOffset = maxOffset;
                    if (newestOffset <= request.getPullFromThisOffset()) {
                        newestOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, queueId);
                    }
                    // 4.2 todo 如果消息队列的最大有效偏移量 大于 消息请求的待拉取偏移量(pullFromThisOffset)，则说明有消息到达，则唤醒请求
                    if (newestOffset > request.getPullFromThisOffset()) {
                        // 4.3 对消息根据 tag HashCode ，使用 MessageFilter 进行一次消息过滤，如果 tag HashCode 属性为空，则直接返回 true
                        boolean match = request.getMessageFilter().isMatchedByConsumeQueue(tagsCode,
                            new ConsumeQueueExt.CqExtUnit(tagsCode, msgStoreTime, filterBitMap));
                        // match by bit map, need eval again when properties is not null.
                        // 如果消息属性不为空 && 根据 tagCode 判断为 true
                        // 则使用 SQL92 过滤器进行消息过滤，如果消息为空，直接返回 true
                        if (match && properties != null) {
                            match = request.getMessageFilter().isMatchedByCommitLog(null, properties);
                        }
                        // 4.4 todo 对应的消息队列有有消息到达，但是可能是不感兴趣的消息(不符合 tag 或者 SQL92 规则) 如果是感兴趣的消息，则针对该请求拉取消息，结束长轮询
                        if (match) {
                            try {
                                // 再次尝试去拉取消息，将拉取结果响应给客户端，实际是上提交到线程池，不会存在性能上的问题
                                this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                    request.getRequestCommand());
                            } catch (Throwable e) {
                                log.error("execute request when wakeup failed.", e);
                            }
                            // 一旦处理过了请求，则跳出循环，进行下一个拉取消息请求的处理，避免加入 replayList ，即避免重复处理
                            continue;
                        }
                    }

                    // todo 4.5 即使没有消息到达或者没有匹配到消息，但是如果超过挂起时间，会再次尝试拉取消息，结束长轮询，避免长时间挂起
                    // todo 这也是为什么短轮询设置的挂起时间是 1s ,是为了保证不管有没有消息到达或匹配到，都去尝试拉取消息
                    if (System.currentTimeMillis() >= (request.getSuspendTimestamp() + request.getTimeoutMillis())) {
                        try {
                            this.brokerController.getPullMessageProcessor().executeRequestWhenWakeup(request.getClientChannel(),
                                request.getRequestCommand());
                        } catch (Throwable e) {
                            log.error("execute request when wakeup failed.", e);
                        }
                        // 一旦处理过了请求，则跳出循环，进行下一个拉取消息请求的处理，避免加入 replayList ，即避免重复处理
                        continue;
                    }
                    // 4.6 不符合唤醒的请求，则将拉取请求重新放入，待下一次检测
                    replayList.add(request);
                }

                // 重新添加回去
                if (!replayList.isEmpty()) {
                    mpr.addPullRequest(replayList);
                }
            }
        }
    }
}
