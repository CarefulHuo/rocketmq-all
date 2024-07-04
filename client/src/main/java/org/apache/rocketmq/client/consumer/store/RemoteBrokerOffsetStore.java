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
package org.apache.rocketmq.client.consumer.store;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 集群模式消费进度存储，即 Consumer 消费进度管理，负责从 Broker 获取消费进度，同步消费进度到 Broker
 * - 获取某个 MessageQueue 的消费进度，可以从内存中获取，也可以从 Broker 获取(获取后更新客户端缓存)
 * - 定期将本地缓存的消费进度上报到 Broker，Broker 会每 5s 将消息消费偏移量持久化到磁盘文件。
 * <p>
 * Remote storage implementation
 */
public class RemoteBrokerOffsetStore implements OffsetStore {
    private final static InternalLogger log = ClientLogger.getLog();

    /**
     * MQ 客户端实例，该实例被同一个客户端的消费者，生产者共用。
     */
    private final MQClientInstance mQClientFactory;

    /**
     * 消费组名称
     */
    private final String groupName;

    /**
     * 内存中的消费进度
     * todo 说明：是针对消息队列的逻辑偏移量
     */
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable =
        new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public RemoteBrokerOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
    }

    /**
     * 集群模式下，不需要加载，实际读取消费进度时，从 Broker 获取即可
     */
    @Override
    public void load() {
    }

    /**
     * 更新消息队列的消费进度(内存中的消费进度)
     * @param mq
     * @param offset
     * @param increaseOnly
     */
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            // 获取传入的 MessageQueue 的消费进度
            AtomicLong offsetOld = this.offsetTable.get(mq);

            // 没有的话，缓存该进度
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            // 如果不为空，说明同时对一个消息队列的消息进行消费，并发执行
            if (null != offsetOld) {

                // 根据 increaseOnly 更新原先的 Offset 值
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    offsetOld.set(offset);
                }
            }
        }
    }

    /**
     * 读取消息队列的消费进度
     *
     * @param mq
     * @param type
     * @return
     */
    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            switch (type) {
                // 先从内存中读取，读取不到，再尝试从磁盘中读取
                case MEMORY_FIRST_THEN_STORE:
                // 从内存中读取
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) {
                        return -1;
                    }
                }
                // 从磁盘中读取
                case READ_FROM_STORE: {
                    try {
                        // todo 12.3.7.5 从 Broker 中获取消息消费的偏移量，如果没有 offset ，No offset in broker 返回 -1，其他异常返回 -2
                        // todo 从 Broker 中获取消息队列的消费进度，返回的消息进度是逻辑偏移量
                        long brokerOffset = this.fetchConsumeOffsetFromBroker(mq);
                        AtomicLong offset = new AtomicLong(brokerOffset);

                        // todo 以 Broker 上的消费进度为准，更新内存中的消费进度
                        this.updateOffset(mq, offset.get(), false);
                        return brokerOffset;
                    }
                    // No offset in broker
                    catch (MQBrokerException e) {
                        return -1;
                    }
                    //Other exceptions
                    catch (Exception e) {
                        log.warn("fetchConsumeOffsetFromBroker exception, " + mq, e);
                        return -2;
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    /**
     * 持久化指定消息队列集合的消费进度到 Broker，并移除非指定的消息队列
     *
     * @see MQClientInstance#startScheduledTask() 客户端启动定时线程
     * @param mqs
     */
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        // 未被使用的消息队列集合
        final HashSet<MessageQueue> unusedMQ = new HashSet<MessageQueue>();

        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            AtomicLong offset = entry.getValue();
            // todo 更新 Broker 上的消费进度时，需要先看下内存中该 MessageQueue 的 Offset 是否为空
            if (offset != null) {
                if (mqs.contains(mq)) {
                    try {
                        // RPC 更新消费进度
                        this.updateConsumeOffsetToBroker(mq, offset.get());
                        log.info("[persistAll] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                            this.groupName,
                            this.mQClientFactory.getClientId(),
                            mq,
                            offset.get());
                    } catch (Exception e) {
                        log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
                    }

                    // 添加未被使用的消息队列到 unusedMQ
                } else {
                    unusedMQ.add(mq);
                }
            }
        }

        // 移除未被使用的消息队列
        if (!unusedMQ.isEmpty()) {
            for (MessageQueue mq : unusedMQ) {
                this.offsetTable.remove(mq);
                log.info("remove unused mq, {}, {}", mq, this.groupName);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
        AtomicLong offset = this.offsetTable.get(mq);
        if (offset != null) {
            try {
                this.updateConsumeOffsetToBroker(mq, offset.get());
                log.info("[persist] Group: {} ClientId: {} updateConsumeOffsetToBroker {} {}",
                    this.groupName,
                    this.mQClientFactory.getClientId(),
                    mq,
                    offset.get());
            } catch (Exception e) {
                log.error("updateConsumeOffsetToBroker exception, " + mq.toString(), e);
            }
        }
    }

    public void removeOffset(MessageQueue mq) {
        if (mq != null) {
            this.offsetTable.remove(mq);
            log.info("remove unnecessary messageQueue offset. group={}, mq={}, offsetTableSize={}", this.groupName, mq,
                offsetTable.size());
        }
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());
        }
        return cloneOffsetTable;
    }

    /**
     * 以一种方式更新 Consumer Offset，一旦 Master 关闭，更新为 Slave，这里需要进行优化。
     * Update the Consumer Offset in one way, once the Master is off, updated to Slave, here need to be optimized.
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {
        updateConsumeOffsetToBroker(mq, offset, true);
    }

    /**
     * 更新消费进度到 Broker
     * Update the Consumer Offset synchronously, once the Master is off, updated to Slave, here need to be optimized.
     */
    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException {

        // 根据消息队列所在的 Broker 名称，获取 Broker 地址，优先选择的是主服务器
        // todo 不管消息是从主服务器还是从服务器拉取的，提交消费进度请求，优先选择主服务器，服务端就是接收其偏移量，更新到服务端内存中，然后定时持久化到磁盘文件上
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {

            // 如果没有找到，则从 NameServer 上重新拉取 Topic 路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {

            // 组装好更新消费进度的 RPC 请求头
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            // 消息主题
            requestHeader.setTopic(mq.getTopic());
            // 消费组名称
            requestHeader.setConsumerGroup(this.groupName);
            // 消息队列序号
            requestHeader.setQueueId(mq.getQueueId());
            // 消息队列的逻辑偏移量
            requestHeader.setCommitOffset(offset);

            if (isOneway) {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffsetOneway(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            } else {
                this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(
                    findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
            }
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }

    /**
     * 从磁盘读取消费进度
     * 消费进度是保存在 Broker 哪个地方？
     * Broker 端的 Offset 管理参照 ConsumerOffsetManager 类，保存逻辑其实跟广播模式差不多，Offset 保存的路径：/rocketmq_home/store/config/consumerOffset.json
     * consumerOffset.json 的内容格式如下：
     *{
     *   "offsetTable": {
     *       "%RETRY%demo_consumer@demo_consumer": {0:0},
     *       "hlb_topic@demo_consumer": {0:4, 1:3, 2:5, 3:2}
     *       // 其中 0,1,2,3 是消息队列序号，4,3,5,2 是消息队列对应的消费进度(逻辑偏移量)
     *     }
     * }
     * @param mq
     *
     * @return
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     * @throws MQClientException
     */
    private long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException {

        // 根据 消息队列中的 Broker 名称获取 Broker 地址，然后发送请求
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {

            // 更新 Topic 路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            // 再次查找 Broker 地址
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.groupName);
            requestHeader.setQueueId(mq.getQueueId());
            // todo 12.3.7.6 RPC调用，获取服务器上消息队列消费的偏移量 也就是获取 Queue的 offset
            // 从 Broker 上拉取执行 Topic下某个消费组下的某个消息队列的消费进度
            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(
                findBrokerResult.getBrokerAddr(), requestHeader, 1000 * 5);
        } else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }
}
