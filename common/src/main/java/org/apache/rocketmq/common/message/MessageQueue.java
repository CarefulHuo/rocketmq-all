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
package org.apache.rocketmq.common.message;

import java.io.Serializable;

/**
 * 消息队列
 * 说明：
 * 1. 每个 Topic的队列分散在不同的 Broker 上，默认情况下 Topic 在 Broker 中对应 4个写队列，4个读队列
 * 2. 在物理层面上，只有写队列才会创建文件
 *    举例：- 写队列个数是 8，读队列个数是 4，这个时候，会创建8个文件夹，代表0,1,2,3,4,5,6,7 但在消息消费时，路由信息只返回 4，
 *         在具体拉取消息时，就只会消费 0,1,2,3 这四个队列中的消息，4,5,6,7中的消息压根不会被消费。
 *         - 反过来，如果写队列个数是4，读队列个数是8，在生产消息时，只会往0,1,2,3 四个队列中生产消息，
 *         消费消息时则会从 0,1，2，3，4，5，6，7 所有的队列中进行消费，当然4,5,6,7 中根本没有消息。
 *         - 假设有两个消费者，按照默认的队列分配策略，事实上只有第一个消费者在真正的消费消息(0,1,2,3)，第二个消费者压根就消费不到消息
 * 3. 由此可见，只有 readQueueNums >= writeQueueNums 才能保证消息全部被消费到，最佳实践是 readQueueNums == writeQueueNums
 * 4. rocketmq设置读写队列数的目的在于方便队列的缩容和扩容。思考一个问题：
 *    - 一个 Topic 在每个 Broker上创建了 128 个队列，现在需要将队列缩容到 64个，怎么做才能保证100%不丢失消息，并且无需重启应用程序？
 *      最佳实践是，先缩容写队列到 64个，等到 64到 127 这 64个读队列内的消息全部被消费了，再缩容读队列到 64个。
 *      同时缩容写队列和读队列，会造成部分消息未被消费
 */
public class MessageQueue implements Comparable<MessageQueue>, Serializable {
    private static final long serialVersionUID = 6191200464116433425L;

    /**
     * 消息队列对应的主题
     */
    private String topic;

    /**
     * 消息队列对应的 Broker
     */
    private String brokerName;

    /**
     * 消息队列序号
     */
    private int queueId;

    public MessageQueue() {

    }

    public MessageQueue(String topic, String brokerName, int queueId) {
        this.topic = topic;
        this.brokerName = brokerName;
        this.queueId = queueId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerName == null) ? 0 : brokerName.hashCode());
        result = prime * result + queueId;
        result = prime * result + ((topic == null) ? 0 : topic.hashCode());
        return result;
    }

    /**
     * 比较两个 MessageQueue 是否相同
     * todo 注意比较 两个 MessageQueue 的比较条件
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MessageQueue other = (MessageQueue) obj;
        if (brokerName == null) {
            if (other.brokerName != null)
                return false;
        } else if (!brokerName.equals(other.brokerName))
            return false;
        if (queueId != other.queueId)
            return false;
        if (topic == null) {
            if (other.topic != null)
                return false;
        } else if (!topic.equals(other.topic))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MessageQueue [topic=" + topic + ", brokerName=" + brokerName + ", queueId=" + queueId + "]";
    }

    @Override
    public int compareTo(MessageQueue o) {
        {
            int result = this.topic.compareTo(o.topic);
            if (result != 0) {
                return result;
            }
        }

        {
            int result = this.brokerName.compareTo(o.brokerName);
            if (result != 0) {
                return result;
            }
        }

        return this.queueId - o.queueId;
    }
}
