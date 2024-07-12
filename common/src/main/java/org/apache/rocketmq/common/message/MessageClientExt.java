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

/**
 * 消息Id
 * <p>
 * todo
 *   1. msgId 是消息发送方 发送消息到 Broker 前生成的唯一Id，存储在消息的附加属性中
 *   2. OffsetMsgId：Broker端存储消息后会生成，并返回给消息发送方，但是，Broker 不会存该属性，因为该Id是可以计算的：在消费端解析消息拉取结果时，就是计算得到的
 * </p>
 */
public class MessageClientExt extends MessageExt {

    public String getOffsetMsgId() {
        return super.getMsgId();
    }

    public void setOffsetMsgId(String offsetMsgId) {
        super.setMsgId(offsetMsgId);
    }

    /**
     * 重写方法
     * <p>
     *  1. 返回的是我们通常所说的 msgId , 消息全局唯一Id，会存储在消息的附加属性 UNIQ_KEY 中
     *  2. 如果消息消费失败，需要重试(顺序消息不是重新投递到 Broker 的方式)，那么 RocketMQ 的做法是将消息重新发送到 Broker 服务器上
     *     此时，全局唯一Id msgId 是不会变的，但是 OffsetMsgId 会发生变化，因为其存储在服务器(broker)上的位置发生了变化
     * @return
     */
    @Override
    public String getMsgId() {
        // msgId
        String uniqID = MessageClientIDSetter.getUniqID(this);

        // 没有 msgId 返回 OffsetMsgId
        if (uniqID == null) {
            return this.getOffsetMsgId();
        } else {
            return uniqID;
        }
    }

    public void setMsgId(String msgId) {
        //DO NOTHING
        //MessageClientIDSetter.setUniqID(this);
    }
}
