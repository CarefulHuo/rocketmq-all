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

package org.apache.rocketmq.client.latency;

import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * RocketMQ 消息发送容错策略，延时实现的门面类
 * <p>
 * todo 特别说明：
 * 1. 开启和不开启延迟规避机制，在消息发送时，都能再不同程度上规避故障的 Broker
 * 2. 区别
 *  - 开启延迟规避机制，其实是一种悲观的做法，一旦消息发送失败后，就会悲观的认为 Broker 不可用，把这个 Broker 记录下，
 *    在接下来的一段时间(延迟规避时间)内，不能再向其发送消息，直接避开该 Broker
 *  - 关闭延迟规避机制，只会在本地消息发送的重试过程中规避该 Broker，下一次消息发送还是个无状态，还是会继续尝试。
 * 3. 联系
 *  - 开启延迟规避机制，是将认为出现问题的 Broker 记录下来，指定多长时间内不能参与消息队列负载
 *  - 关闭延迟规避机制，只是对上次发送该条消息出现的故障进行规避
 * <p>
 * 默认情况下容错策略关闭，即 sendLatencyFaultEnable = false
 */
public class MQFaultStrategy {
    private final static InternalLogger log = ClientLogger.getLog();

    /**
     * 延迟故障容错，维护每个 Broker 的发送消息的延迟
     */
    private final LatencyFaultTolerance<String> latencyFaultTolerance = new LatencyFaultToleranceImpl();

    /**
     * 发送消息延迟容错开关
     */
    private boolean sendLatencyFaultEnable = false;

    /*
      | Producer 发送消息耗时 | Broker 不可用时长 |
      |---------------------|-----------------|
      | >= 15 * 1000ms      | 600 * 1000ms    |
      | >=  3 * 1000ms      | 180 * 1000ms    |
      | >=  2 * 1000ms      | 120 * 1000ms    |
      | >=  1 * 1000ms      |  60 * 1000ms    |
      | >= 550ms            |  30 * 1000ms    |
      | >= 100ms            |  0ms            |
      | >= 50ms             |  0ms            |
     */
    /**
     * 延迟时间数组 单位:ms
     */
    private long[] latencyMax = {50L, 100L, 550L, 1000L, 2000L, 3000L, 15000L};

    /**
     * 不可用时长数组 单位：ms
     */
    private long[] notAvailableDuration = {0L, 0L, 30000L, 60000L, 120000L, 180000L, 600000L};

    public long[] getNotAvailableDuration() {
        return notAvailableDuration;
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.notAvailableDuration = notAvailableDuration;
    }

    public long[] getLatencyMax() {
        return latencyMax;
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.latencyMax = latencyMax;
    }

    public boolean isSendLatencyFaultEnable() {
        return sendLatencyFaultEnable;
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.sendLatencyFaultEnable = sendLatencyFaultEnable;
    }

    /**
     * 容错策略选择消息队列逻辑
     * <p>
     * 根据 Topic 发布信息选择一个消息队列，主要逻辑如下：
     * 1. 开启容错策略，选择消息队列逻辑，
     *   - 最优-在 topic 发布信息中，直接选择可用消息队列
     *   - 其次-在延迟容错的 Broker 中选择一个 Broker，选择一个写消息队列
     *   - 最坏-返回任意一个 Broker 上的写消息队列
     * 2. 关闭容错策略，选择消息队列逻辑，会按照随机递增取模的算法，获取一个 MessageQueue 进行消息发送，但是会避开上次消息发送失败的 Broker
     * @param tpInfo
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        // 默认情况下容错策略关闭
        // 是否开启失败延迟规避机制，该值可以在消息发送者那里设置
        // 开启容错策略
        if (this.sendLatencyFaultEnable) {
            try {
                /**
                 * 优选获取可用写消息队列，选择的队列的 Broker 是可用的
                 */
                // 使用了本地线程变量 ThreadLocal 保存上一次发送消息使用的消息队列下标，消息发送使用轮询机制获取下一个发送消息使用的写队列
                int index = tpInfo.getSendWhichQueue().incrementAndGet();

                // 对 Topic 下所有写消息队列，进行一次 Broker 是否可用的验证，因为开启了发送异常延迟容错，要确保选中的消息队列(MessageQueue)所在的 Broker 是可用的
                for (int i = 0; i < tpInfo.getMessageQueueList().size(); i++) {
                    int pos = Math.abs(index++) % tpInfo.getMessageQueueList().size();
                    if (pos < 0)
                        pos = 0;
                    MessageQueue mq = tpInfo.getMessageQueueList().get(pos);

                    // 判断选中的 MessageQueue 所在的 Broker 是否可用，如果可用，则立即返回
                    // todo 注意：Topic 所有的写消息队列，所在的Broker，全部被标记为不可用时，进入下一步处理逻辑
                    //  (在这里，要明白，不是标记了 Broker 不可用，就代表真的不可用。Broker 是可以在故障期间被运营管理人员进行恢复的，比如重启等措施)
                    //  失败延时规避机制就体现在这里，如果消息发送者遇到一次消息发送失败后，就会悲观的认为 Broker 不可用。
                    //  在接下来的一段时间内，该 Broker 就不能参与消息发送队列的负载，直到该队列所在的 Broker 可用
                    if (latencyFaultTolerance.isAvailable(mq.getBrokerName()))
                        return mq;
                }

                /**
                 * 选择一个相对较好的 Broker 来获取消息队列，不考虑消息队列所在的 Broker 是否可用
                 */

                // 根据 Broker 的 消息发送时长 进行一个排序，值越小，越排前面，然后选择一个 Broker 返回。
                // 此时不能保证一定可用，如果抛出异常，消息发送方式时同步调用还是异步调用，都会进行重试
                final String notBestBroker = latencyFaultTolerance.pickOneAtLeast();

                // 根据 Broker 名称获取该 Topic 在该 Broker 上的写队列数量
                int writeQueueNums = tpInfo.getQueueIdByBroker(notBestBroker);

                // 如果写队列数量大于 0
                if (writeQueueNums > 0) {
                    // 根据随机递增取模算法，在归属该 Topic 所有的写队列里面，取出一个队列
                    final MessageQueue mq = tpInfo.selectOneMessageQueue();

                    // 如果 Broker 名称不为空，将选中的消息队列的 Broker 名称设置为该 Broker 的名称，并对选中消息队列的序号重新进行取模计算
                    // 因为选中的写队列，不一定位于这个 Broker 上，而且写队列数量，也可能不对
                    if (notBestBroker != null) {
                        mq.setBrokerName(notBestBroker);
                        mq.setQueueId(tpInfo.getSendWhichQueue().incrementAndGet() % writeQueueNums);
                    }
                    return mq;

                    // 删除这个 Broker
                } else {
                    latencyFaultTolerance.remove(notBestBroker);
                }
            } catch (Exception e) {
                log.error("Error occurred when selecting message queue", e);
            }

            /**
             * 选择任意一个 Broker 上的写队列返回，不考虑 Broker 是否可用
             */
            // todo 基于随机递增取模算法，从 Topic 的发布路由信息中选择一个消息队列，不考虑队列的可用性
            return tpInfo.selectOneMessageQueue();
        }

        // 关闭延迟规避机制
        // 如果 lastBrokerName 不为空，则会规避上次发送失败的 Broker，否则就是直接从 Topic 的所有写队列中选择一个
        return tpInfo.selectOneMessageQueue(lastBrokerName);
    }

    /**
     * 更新延迟容错信息
     * 说明：当 Producer 发送消息时间过长，则逻辑认为 N 秒内不可用
     *
     * @param brokerName Broker 名称
     * @param currentLatency 延迟时间(消息发送耗时)
     * @param isolation 是否隔离，当开启隔离时，默认不可用时长为 30000，目前主要用于发送消息异常时
     */
    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        // 延迟规避机制开启
        if (this.sendLatencyFaultEnable) {
            // 计算延迟对应的不可用时长
            // 1. 如果隔离，则使用 30s 作为消息发送延迟时间，即当前 Broker 在接下来的 60000L 也就是 5 分钟内不提供服务
            // 2. 如果不隔离，则使用本次消息发送耗时作为消息发送延迟时间
            long duration = computeNotAvailableDuration(isolation ? 30000 : currentLatency);
            this.latencyFaultTolerance.updateFaultItem(brokerName, currentLatency, duration);
        }
    }

    /**
     * 计算消息发送延迟时间对应的 Broker 不可用时间，计算方法如下：
     * 1. 根据消息发送的延迟时间，从 latencyMax 数组尾部向前遍历，找到第一个小于等于 currentLatency 的索引下标 i，如果没有找到返回 0 ;
     * 2. 找到的话，根据这个索引从 notAvailableDuration 数组中取出对应的时间，作为 Broker 不可用时间
     * @param currentLatency 消息发送耗时时间
     * @return
     */
    private long computeNotAvailableDuration(final long currentLatency) {
        for (int i = latencyMax.length - 1; i >= 0; i--) {
            if (currentLatency >= latencyMax[i])
                return this.notAvailableDuration[i];
        }

        return 0;
    }
}
