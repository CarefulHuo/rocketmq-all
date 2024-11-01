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
package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.topic.TopicValidator;

import java.nio.charset.Charset;

/**
 * 事务消息工具类
 */
public class TransactionalMessageUtil {

    /**
     * 删除操作
     */
    public static final String REMOVETAG = "d";
    public static Charset charset = Charset.forName("utf-8");

    /**
     * 返回操作事务消息主题：RMQ_SYS_TRANS_OP_HALF_TOPIC
     * @return
     */
    public static String buildOpTopic() {
        return TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC;
    }

    /**
     * 返回半消息的主题：RMQ_SYS_TRANS_HALF_TOPIC
     * @return
     */
    public static String buildHalfTopic() {
        return TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
    }

    /**
     * 返回消费组
     * todo 注意：这个是内部消费组，专门用来消费{@link TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC} 和 {@link TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC} 主题下的消息
     * @return
     */
    public static String buildConsumerGroup() {
        return MixAll.CID_SYS_RMQ_TRANS;
    }

}
