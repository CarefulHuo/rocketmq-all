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

import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消费端消息进度存储
 * - 用来管理当前 OffsetStore 对应的消费端的每个消费队列的不同消费组的消费进度。对于一个新的消费组，无论是集群模式还是广播模式都不会存储该消费组的消费进度，可以理解为 -1.
 * <p>
 * 广播模式：
 * - 同消费组的消费者相互独立，消费进度要单独存储；即每条消息要被每一个消费者消费，则消费进度可以与消费者保存在一起，也就是本地存储。
 * 集群模式：
 * - 同一条消息只会被同一个消费组消费一次，消费进度与负载均衡有关，因为消费进度需要共享
 * - Broker 管理的消息队列的偏移量，是针对某个消费组的 Topic 下每个消息队列的进度
 * - 当前 OffsetStore 对应的消费端的进度会定时上报到 Broker
 * <p>
 *  对 OffsetStore 的管理分为本地模式和远程模式，本地模式是以文本文件的形式存储在客户端，而远程模式是将数据保存在 Broker端
 *  对应数据结构分别为 LocalFileOffsetStore 和 RemoteBrokerOffsetStore
 *  1. 默认情况下，当消费模式为广播模式时，Offset使用本地模式存储，因为每条消息会被所有的消费者消费，每个消费者管理自己的消费进度，各个消费者之间不存在消费进度的交集；
 *  2. 当消费模式为集群模式时，则使用远程模式管理 Offset，消息会被一个消费者消费，不同的是每个消费者只会负责消费其中部分消息队列，添加或删除消费者，都会导致负载发生变动，容易造成消费进度冲突，因此需要集中管理。
 * <p>
 *  说明：
 *  1. RemoteBrokerOffsetStore：集群模式下，使用远程 Broker 管理消费进度
 *  2. LocalFileOffsetStore：广播模式下，使用本地文件消费进度
 *  3. todo 消费进度持久化不仅仅只有定时持久化，拉取消息，分配消息队列等等操作，都会进行消息进度持久化
 * Offset store interface
 */
public interface OffsetStore {
    /**
     * 加载消费进度到内存
     * Load
     */
    void load() throws MQClientException;

    /**
     * 更新内存中的消费进度
     * Update the offset,store it in memory
     */
    void updateOffset(final MessageQueue mq, final long offset, final boolean increaseOnly);

    /**
     * Get offset from local storage
     * 读取消费进度类型
     * - {@link ReadOffsetType#READ_FROM_MEMORY} 从内存中读取
     * - {@link ReadOffsetType#READ_FROM_STORE} 从存储(Broker 或 文件)中读取
     * - {@link ReadOffsetType#MEMORY_FIRST_THEN_STORE} 优先从内存中读取，读取不到，从文件中读取
     * @return The fetched offset
     */
    long readOffset(final MessageQueue mq, final ReadOffsetType type);

    /**
     * 持久化所有消费进度
     * Persist all offsets,may be in local storage or remote name server
     */
    void persistAll(final Set<MessageQueue> mqs);

    /**
     * 持久化该条消息队列的消费进度
     * Persist the offset,may be in local storage or remote name server
     */
    void persist(final MessageQueue mq);

    /**
     * 删除该条消息队列的消费进度
     * Remove offset
     */
    void removeOffset(MessageQueue mq);

    /**
     * 根据消息主题，克隆该 Topic 下所有消息队列的消费进度
     * @return The cloned offset table of given topic
     */
    Map<MessageQueue, Long> cloneOffsetTable(String topic);

    /**
     * 使用集群模式的情况下，更新存储在 Broker端的消息消费进度，
     * @param mq
     * @param offset
     * @param isOneway
     */
    void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
        MQBrokerException, InterruptedException, MQClientException;
}
