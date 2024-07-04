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
 * 队列分配策略 -- 平均分配，这个是默认的分配策略
 * 如果队列数和消费者数量相除有余数时，余数按照顺序 1个 1个 分配消费者
 * 比如：8个队列 q1,q2,q3,q4,q5,q6,q7,q8 消费者3个：c1,c2,c3
 * 分配如下：
 * c1:q1,q2,q3
 * c2:q4,q5,q6
 * c3:q7,q8
 * todo 特别声明
 *  平均分配算法，类似于分页的算法，将所有MessageQueue排好序类似于记录，将所有消费端Consumer排好序类似页数，并求出每一页需要包含的平均size和每个页面记录的范围range，
 *  最后遍历整个range而计算出当前Consumer端应该分配到的记录（这里即为：MessageQueue）。
 *  也就是：每个消费者平均分配队列，如果队列数不能被消费者数整除，则余数队列分配给最后一个消费者
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 平均分配队列策略
     *
     * @param consumerGroup current consumer group 消费组
     * @param currentCID    current consumer id  要分配队列的消费者Id
     * @param mqAll         message queue set in current topic Topic下的消息队列
     * @param cidAll        consumer set in current consumer group 消费者集合
     * @return
     */
    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {

        // 检查参数是否正确
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        // 分配后的消息队列，如果消费者集合不包含该消费者，直接返回空的消息队列
        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        // 平均分配
        // 当前 Consumer 在消费者组内是第几个 Consumer
        // 这里就是为什么需要对传入的 cidAll 参数进行排序的原因，如果不排序，Consumer 在本地计算出来的 Index 无法一致，影响计算结果
        int index = cidAll.indexOf(currentCID);

        // 余数，即会有多少个消息队列无法被平均分配
        int mod = mqAll.size() % cidAll.size();

        // 消息队列总数 <= 消费者总数，分配当前消费者 1个消息队列
        // 不能均分 && 当前消费者序号(从 0 开始) < 余下的队列数(mod)，分配当前消费者 maAll / cidAll + 1 个队列
        // 不能均分 && 当前消费者序号(从 0 开始) >= 余下的队列数(mod)，分配当前消费者 maAll / cidAll 个队列
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());

        // 有余数的情况下，[0,mod) 平分余数，即每 Consumer 每多分一个节点，第 index 开始，跳过前 mod 余数
        // Consumer 分配消息队列开始的位置
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;

        // 每个消费者平均分配队列，如果队列数不能被消费者数整除，则余数队列分配给最后一个消费者
        // 分配队列数据，之所以要 Math.min()的原因是：mqAll.size() <= cidAll.size()，是为了让部分Consumer 分配不到消息队列
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }

        // 分配消息队列结果
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
