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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 消费组下的消费者上报信息
 */
public class ConsumerGroupInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 消费组名称
     */
    private final String groupName;

    /**
     * 消费者订阅关系集合
     * todo 注意：同一个消费组下的不同消费者如果订阅的 Topic 相同，最终会进行覆盖式替换之前的 Topic 对应的订阅信息，也就是以最后一个上报的消费者的订阅信息为准，忽略 tag 的不一致
     */
    private final ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable =
            new ConcurrentHashMap<String, SubscriptionData>();

    /**
     * 消息通信
     * key: 消费者客户端连接当前 Broker 的通道
     * value:消费者客户端对象信息
     */
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
            new ConcurrentHashMap<Channel, ClientChannelInfo>(16);

    /**
     * 消费模式
     */
    private volatile ConsumeType consumeType;

    /**
     * 消息模式
     */
    private volatile MessageModel messageModel;

    /**
     * 从哪里开始消费
     */
    private volatile ConsumeFromWhere consumeFromWhere;

    /**
     * 消费组信息上次更新时间
     */
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel,
                             ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    public ClientChannelInfo findChannel(final String clientId) {
        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> next = it.next();
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }

        return null;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public ConcurrentMap<Channel, ClientChannelInfo> getChannelInfoTable() {
        return channelInfoTable;
    }

    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }

    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();

        Iterator<Entry<Channel, ClientChannelInfo>> it = this.channelInfoTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<Channel, ClientChannelInfo> entry = it.next();
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }

    public void unregisterChannel(final ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    public boolean doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        final ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn(
                    "NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}",
                    info.toString(), groupName);
            return true;
        }

        return false;
    }

    public boolean updateChannel(final ClientChannelInfo infoNew, ConsumeType consumeType,
                                 MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;

        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        if (null == infoOld) {
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType,
                        messageModel, infoNew.toString());
                updated = true;
            }

            infoOld = infoNew;
        } else {
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ",
                        this.groupName,
                        infoOld.toString(),
                        infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);

        return updated;
    }

    /**
     * 更新 Topic 的订阅信息
     * 对传入的消费者订阅信息集合，进行处理，更新 subscriptionTable 表，此方法会检查每个 Topic 的订阅是否存在
     * 如果不存在就添加 Topic 的对应订阅信息，如果存在则比较订阅信息的版本，进行更新，同时，移除 subscriptionTable 表中不在传入集合中存在的订阅信息
     * @param subList
     * @return
     */
    public boolean updateSubscription(final Set<SubscriptionData> subList) {
        boolean updated = false;

        // 遍历订阅列表，对每一个订阅进行处理
        for (SubscriptionData sub : subList) {
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            // 检查订阅是否存在
            if (old == null) {
                // 不存在，则创建订阅信息
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                if (null == prev) {
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}",
                            this.groupName,
                            sub.toString());
                }
            } else if (sub.getSubVersion() > old.getSubVersion()) {
                // 存在，则比较版本，进行订阅信息覆盖
                // todo 即如果同一个消费组下订阅的 Topic 相同，即使 tag 不一致，则以最后一个消费者订阅信息为准
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}",
                            this.groupName,
                            old.toString(),
                            sub.toString()
                    );
                }

                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }

        // 遍历订阅信息表 subscriptionTable  将不存在于 subList 中的订阅信息，进行移除
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();

            boolean exist = false;
            for (SubscriptionData sub : subList) {
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            if (!exist) {
                log.warn("subscription changed, group: {} remove topic {} {}",
                        this.groupName,
                        oldTopic,
                        next.getValue().toString()
                );

                it.remove();
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }

    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    public SubscriptionData findSubscriptionData(final String topic) {
        return this.subscriptionTable.get(topic);
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public String getGroupName() {
        return groupName;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
