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
package org.apache.rocketmq.broker.offset;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 消费进度管理器(消费者消息消费进度管理器)
 */
public class ConsumerOffsetManager extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * Topic 与 消费组 的分隔符
     */
    private static final String TOPIC_GROUP_SEPARATOR = "@";

    /**
     * todo 消费进度集合。。针对某个 Topic 为不同消费组维护消费进度
     * todo key1 是 topic@group 以 Topic 和消费组方式维护的。即每个 Topic 下不同消费组的消费进度
     * todo key2 是 queueId，value 是为消费位移(这里不是 offset 而是逻辑偏移量)
     */
    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable =
        new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>(512);

    private transient BrokerController brokerController;

    public ConsumerOffsetManager() {
    }

    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 该函数用于扫描未订阅的主题。具体而言，它遍历offsetTable（一个映射表，其中key为topic和group的组合，value为偏移量）的条目，并根据以下条件删除条目：
     * 将条目的key按TOPIC_GROUP_SEPARATOR分割成两个部分：topic和group。
     * 检查该group是否订阅了该topic。
     * 如果未订阅且该主题的偏移量比某个阈值更旧，则删除该条目并记录日志。
     * 这样可以确保offsetTable中只保留被订阅的主题的偏移量。
     */
    public void scanUnsubscribedTopic() {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                String topic = arrays[0];
                String group = arrays[1];

                if (null == brokerController.getConsumerManager().findSubscriptionData(group, topic)
                    && this.offsetBehindMuchThanData(topic, next.getValue())) {
                    it.remove();
                    log.warn("remove topic offset, {}", topicAtGroup);
                }
            }
        }
    }

    /**
     * 该函数用于判断给定主题(topic)的消费位点(offset)是否都落后于存储中的最小位点(minOffsetInStore)。
     * 函数遍历 table 中的每个队列的位点，如果任一队列的消费位点大于存储中的最小位点，则返回false，否则返回true。
     * @param topic
     * @param table
     * @return
     */
    private boolean offsetBehindMuchThanData(final String topic, ConcurrentMap<Integer, Long> table) {
        Iterator<Entry<Integer, Long>> it = table.entrySet().iterator();
        boolean result = !table.isEmpty();

        while (it.hasNext() && result) {
            Entry<Integer, Long> next = it.next();
            long minOffsetInStore = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, next.getKey());
            long offsetInPersist = next.getValue();
            result = offsetInPersist <= minOffsetInStore;
        }

        return result;
    }

    /**
     * 通过传入的消费者组名，返回该消费者组订阅的所有主题名称的集合
     * @param group
     * @return
     */
    public Set<String> whichTopicByConsumer(final String group) {
        Set<String> topics = new HashSet<String>();

        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (group.equals(arrays[1])) {
                    topics.add(arrays[0]);
                }
            }
        }

        return topics;
    }

    /**
     * 根据给定的topic，返回与此topic相关联的所有group名称的集合
     *
     * @param topic
     * @return
     */
    public Set<String> whichGroupByTopic(final String topic) {
        Set<String> groups = new HashSet<String>();

        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
            if (arrays.length == 2) {
                if (topic.equals(arrays[0])) {
                    groups.add(arrays[1]);
                }
            }
        }

        return groups;
    }

    /**
     * 提交消费进度
     * todo 基于某个 Topic 为每个消费组维护一份消费进度，消费进度基于 queue
     * @param clientHost 提交 Client 地址
     * @param group  消费者组
     * @param topic 消费主题
     * @param queueId 队列编号
     * @param offset  消费进度(队列位置)
     */
    public void commitOffset(final String clientHost, final String group, final String topic, final int queueId,
        final long offset) {
        // todo 1. topic@group  为 某个topic 下每个消费组维护一份消费进度
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        // 2. 保存消费进度
        this.commitOffset(clientHost, key, queueId, offset);
    }

    /**
     * 提交消费进度
     * todo 1 每次 Consumer 进行 RPC 调用上报自己的消费进度，Broker 接收后，并没有立即进行持久化，而是直接更新到内存中
     * todo 2 具体的持久化，是通过 Broker 启动时开启的定时任务，在 BrokerController#initialize() 方法中
     *
     * @param clientHost 提交 Client 地址
     * @param key        主题@消费者组
     * @param queueId    队列编号
     * @param offset     进度(队列位置)
     */
    private void commitOffset(final String clientHost, final String key, final int queueId, final long offset) {
        // 从缓存中获取队列的消费进度
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<Integer, Long>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        } else {
            // ConcurrentMap#put() 会返回之前的值
            Long storeOffset = map.put(queueId, offset);
            if (storeOffset != null && offset < storeOffset) {
                log.warn("[NOTIFYME]update consumer offset less than store. clientHost={}, key={}, queueId={}, requestOffset={}, storeOffset={}", clientHost, key, queueId, offset, storeOffset);
            }
        }
    }

    /**
     * 查询队列的偏移量
     * @param group 消费者组
     * @param topic 消息主题
     * @param queueId 队列编号
     * @return
     */
    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null)
                return offset;
        }

        // 如果没有对应的队列，返回 -1
        return -1;
    }

    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getConsumerOffsetPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    /**
     * 解码内容
     * @param jsonString 内容
     */
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            // 获取文件内容后，反序列化为 ConsumerOffsetManager 对象
            // 实质是为了重新载入其属性 ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable
            ConsumerOffsetManager obj = RemotingSerializable.fromJson(jsonString, ConsumerOffsetManager.class);
            if (obj != null) {
                this.offsetTable = obj.offsetTable;
            }
        }
    }

    /**
     * 编码内容，即将当前内存数据写入到文件，也就是将 ConsumerOffsetManager 对象序列化后写入到文件
     *
     * @param prettyFormat 是否格式化
     * @return
     */
    public String encode(final boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, Long>> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentHashMap<String, ConcurrentMap<Integer, Long>> offsetTable) {
        this.offsetTable = offsetTable;
    }

    public Map<Integer, Long> queryMinOffsetInAllGroup(final String topic, final String filterGroups) {

        Map<Integer, Long> queueMinOffset = new HashMap<Integer, Long>();
        Set<String> topicGroups = this.offsetTable.keySet();
        if (!UtilAll.isBlank(filterGroups)) {
            for (String group : filterGroups.split(",")) {
                Iterator<String> it = topicGroups.iterator();
                while (it.hasNext()) {
                    if (group.equals(it.next().split(TOPIC_GROUP_SEPARATOR)[1])) {
                        it.remove();
                    }
                }
            }
        }

        for (Map.Entry<String, ConcurrentMap<Integer, Long>> offSetEntry : this.offsetTable.entrySet()) {
            String topicGroup = offSetEntry.getKey();
            String[] topicGroupArr = topicGroup.split(TOPIC_GROUP_SEPARATOR);
            if (topic.equals(topicGroupArr[0])) {
                for (Entry<Integer, Long> entry : offSetEntry.getValue().entrySet()) {
                    long minOffset = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, entry.getKey());
                    if (entry.getValue() >= minOffset) {
                        Long offset = queueMinOffset.get(entry.getKey());
                        if (offset == null) {
                            queueMinOffset.put(entry.getKey(), Math.min(Long.MAX_VALUE, entry.getValue()));
                        } else {
                            queueMinOffset.put(entry.getKey(), Math.min(entry.getValue(), offset));
                        }
                    }
                }
            }

        }
        return queueMinOffset;
    }

    public Map<Integer, Long> queryOffset(final String group, final String topic) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        return this.offsetTable.get(key);
    }

    public void cloneOffset(final String srcGroup, final String destGroup, final String topic) {
        ConcurrentMap<Integer, Long> offsets = this.offsetTable.get(topic + TOPIC_GROUP_SEPARATOR + srcGroup);
        if (offsets != null) {
            this.offsetTable.put(topic + TOPIC_GROUP_SEPARATOR + destGroup, new ConcurrentHashMap<Integer, Long>(offsets));
        }
    }

    public void removeOffset(final String group) {
        Iterator<Entry<String, ConcurrentMap<Integer, Long>>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, Long>> next = it.next();
            String topicAtGroup = next.getKey();
            if (topicAtGroup.contains(group)) {
                String[] arrays = topicAtGroup.split(TOPIC_GROUP_SEPARATOR);
                if (arrays.length == 2 && group.equals(arrays[1])) {
                    it.remove();
                    log.warn("clean group offset {}", topicAtGroup);
                }
            }
        }

    }

}
