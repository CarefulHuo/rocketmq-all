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
package org.apache.rocketmq.client.impl.producer;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

/**
 * Topic 信息 包含以下信息
 * 1. Topic 的队列集合信息
 * 2. Topic 的路由信息
 */
public class TopicPublishInfo {

    /**
     * 消息主题是否是有序的
     */
    private boolean orderTopic = false;

    /**
     * 是否存在 Topic 路由信息，默认为 false
     */
    private boolean haveTopicRouterInfo = false;

    /**
     * Topic的队列集合
     * todo 注意：是某个 Topic 在所有 Broker 主节点上分布的队列。如有 Broker A 和 Broker B，写队列 4 个，那么这里就有 8 个队列
     */
    private List<MessageQueue> messageQueueList = new ArrayList<MessageQueue>();

    /**
     * 保存上次发送的消息队列下标
     */
    private volatile ThreadLocalIndex sendWhichQueue = new ThreadLocalIndex();

    /**
     * Topic 的路由信息(原始信息)，包含队列信息和Broker信息
     */
    private TopicRouteData topicRouteData;

    public boolean isOrderTopic() {
        return orderTopic;
    }

    public void setOrderTopic(boolean orderTopic) {
        this.orderTopic = orderTopic;
    }

    /**
     * Topic 发布信息中包含的队列不为空
     * @return
     */
    public boolean ok() {
        return null != this.messageQueueList && !this.messageQueueList.isEmpty();
    }

    public List<MessageQueue> getMessageQueueList() {
        return messageQueueList;
    }

    public void setMessageQueueList(List<MessageQueue> messageQueueList) {
        this.messageQueueList = messageQueueList;
    }

    public ThreadLocalIndex getSendWhichQueue() {
        return sendWhichQueue;
    }

    public void setSendWhichQueue(ThreadLocalIndex sendWhichQueue) {
        this.sendWhichQueue = sendWhichQueue;
    }

    public boolean isHaveTopicRouterInfo() {
        return haveTopicRouterInfo;
    }

    public void setHaveTopicRouterInfo(boolean haveTopicRouterInfo) {
        this.haveTopicRouterInfo = haveTopicRouterInfo;
    }

    /**
     * 基于递增取模算法，获取对应的 MessageQueue。如果传入了发送消息失败的 Broker ，则要先过滤再考虑基于递增取模算法获取。
     *
     * @param lastBrokerName 上次发送消息失败的 BrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final String lastBrokerName) {
        // BrokerName 为空
        if (lastBrokerName == null) {
            return selectOneMessageQueue();

            // 不为空，增加 Broker 名称做过滤
        } else {
            for (int i = 0; i < this.messageQueueList.size(); i++) {
                int index = this.sendWhichQueue.incrementAndGet();
                int pos = Math.abs(index) % this.messageQueueList.size();
                if (pos < 0)
                    pos = 0;
                MessageQueue mq = this.messageQueueList.get(pos);

                // 过滤掉上次发送消息失败的 Broker
                // todo 此处实现了，即使不开启延迟规避机制，发送时，也可以规避上次消息失败的 Broker
                if (!mq.getBrokerName().equals(lastBrokerName)) {
                    return mq;
                }
            }
            return selectOneMessageQueue();
        }
    }

    /**
     * 选择一个 MessageQueue 来发送消息
     * todo 注意：是基于随机递增取模算法
     * @return
     */
    public MessageQueue selectOneMessageQueue() {
        // 随机递增
        int index = this.sendWhichQueue.incrementAndGet();
        // 取模
        int pos = Math.abs(index) % this.messageQueueList.size();
        if (pos < 0)
            pos = 0;

        // 选择 Topic 下对应的队列
        return this.messageQueueList.get(pos);
    }

    /**
     * 从 Topic 路由信息中获取该 Topic 在所有 Broker 上的所有队列，
     * 并根据 BrokerName 获取该 Topic 分布在这个 Broker 上的写队列数量
     *
     * @param brokerName
     * @return
     */
    public int getQueueIdByBroker(final String brokerName) {
        for (int i = 0; i < topicRouteData.getQueueDatas().size(); i++) {
            final QueueData queueData = this.topicRouteData.getQueueDatas().get(i);
            // 队列的 Broker 名称 与传入的 Broker 名称一致
            if (queueData.getBrokerName().equals(brokerName)) {
                // 返回该 Topic 在 BrokerName 上的写队列数量
                return queueData.getWriteQueueNums();
            }
        }

        return -1;
    }

    @Override
    public String toString() {
        return "TopicPublishInfo [orderTopic=" + orderTopic + ", messageQueueList=" + messageQueueList
            + ", sendWhichQueue=" + sendWhichQueue + ", haveTopicRouterInfo=" + haveTopicRouterInfo + "]";
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public void setTopicRouteData(final TopicRouteData topicRouteData) {
        this.topicRouteData = topicRouteData;
    }
}
