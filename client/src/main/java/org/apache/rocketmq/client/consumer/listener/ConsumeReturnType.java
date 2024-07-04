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

package org.apache.rocketmq.client.consumer.listener;

/**
 * 消费者处理消息后返回的不同结果类型
 */
public enum ConsumeReturnType {
    /**
     * 消费成功，消费者正确处理了消息内容，没有任何错误或异常发生
     * consume return success
     */
    SUCCESS,

    /**
     * 消费超时，尽管消息可能已经成功处理，但由于处理时间过长超过了预设的阈值(消费端的 ConsumeTimeout 配置)，因此被视为一种异常情况
     * consume timeout ,even if success
     */
    TIME_OUT,

    /**
     * 消费过程中抛出了一场。这表明在处理消息时发生了未被捕获的异常，导致消费逻辑未能正常完成
     * consume throw exception
     */
    EXCEPTION,

    /**
     * 消费返回空，消费方法返回了 null，某些消费模式下，消费者可能会根据业务逻辑选择性的返回 null，这通常被视为一种特殊的处理结果，需要特别关注
     * consume return null
     */
    RETURNNULL,

    /**
     * 消费失败，直接指明消息处理未能成功，与 EXCEPTION 的区别在于它可能不涉及抛出异常，而仅仅是一种逻辑上的处理失败状态
     * consume return failed
     */
    FAILED
}
