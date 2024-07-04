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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 环型分配 -- 平均轮询分配
 * 举例：8个队列 q1,q2,q3,q4,q5,q6,q7,q8 ，消费者3个：c1,c2,c3
 * 分配如下：
 * c1:q1,q4,q7
 * c2:q2,q5,q8
 * c3:q3,q6
 *
 * Cycle average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragelyByCircle implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 平均轮询分配
     *
     * @param consumerGroup current consumer group 消费组
     * @param currentCID    current consumer id  消费者Id
     * @param mqAll         message queue set in current topic Topic下的消息队列
     * @param cidAll        consumer set in current consumer group 消费者集合
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        // 检查参数
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        // 获取当前消费者在消费者集合中的索引
        int index = cidAll.indexOf(currentCID);

        // 从 index 开始遍历 mqAll，如果 cidAll.size() > mqAll.size()，则直接返回空的消息队列集合
        for (int i = index; i < mqAll.size(); i++) {
            // 对满足 i % cidAll.size() 求余的结果 == index 的消息队列分配给消费者
            if (i % cidAll.size() == index) {
                result.add(mqAll.get(i));
            }
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG_BY_CIRCLE";
    }
}
