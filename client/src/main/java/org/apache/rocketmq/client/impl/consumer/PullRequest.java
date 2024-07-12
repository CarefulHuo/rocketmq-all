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

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 拉取消息请求
 * todo 说明：
 *  - 每个 MessageQueue 对应封装成了一个 PullRequest，因为拉取数据是以每个 Broker 下面的 Queue 为单位
 *  - ProcessQueue 属性，每个 MessageQueue 对应一个 ProcessQueue，保存了这个 MessageQueue 消息处理状态的快照
 *  - nextOffset 用来标识读取的位置
 *  - consumerGroup 指定了哪个消费者组要拉取消息
 *  特别说明
 *   拉取数据是以每个 Broker下面的 Queue 为单位
 */
public class PullRequest {

    /**
     * 消费者组
     */
    private String consumerGroup;

    /**
     * 待拉取消息队列
     */
    private MessageQueue messageQueue;

    /**
     * 消息处理队列
     */
    private ProcessQueue processQueue;

    /**
     * 待拉取的 MessageQueue 偏移量(逻辑偏移量)
     */
    private long nextOffset;

    /**
     * 之前是否被锁定，在拉取消息时，用于保证 PullRequest 的初始拉取点，只在第一次拉取时设置
     */
    private boolean previouslyLocked = false;

    public boolean isPreviouslyLocked() {
        return previouslyLocked;
    }

    public void setPreviouslyLocked(boolean previouslyLocked) {
        this.previouslyLocked = previouslyLocked;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((consumerGroup == null) ? 0 : consumerGroup.hashCode());
        result = prime * result + ((messageQueue == null) ? 0 : messageQueue.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PullRequest other = (PullRequest) obj;
        if (consumerGroup == null) {
            if (other.consumerGroup != null)
                return false;
        } else if (!consumerGroup.equals(other.consumerGroup))
            return false;
        if (messageQueue == null) {
            if (other.messageQueue != null)
                return false;
        } else if (!messageQueue.equals(other.messageQueue))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "PullRequest [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue
            + ", nextOffset=" + nextOffset + "]";
    }

    public ProcessQueue getProcessQueue() {
        return processQueue;
    }

    public void setProcessQueue(ProcessQueue processQueue) {
        this.processQueue = processQueue;
    }
}
