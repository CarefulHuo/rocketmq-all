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
package org.apache.rocketmq.broker.client;

/**
 * 消费组活动状态
 */
public enum ConsumerGroupEvent {

    /**
     * 消费组内的一些消费者发生改变
     * Some consumers in the group are changed.
     */
    CHANGE,
    /**
     * 消费组未注册
     * The group of consumer is unregistered.
     */
    UNREGISTER,
    /**
     * 消费组已经被注册
     * The group of consumer is registered.
     */
    REGISTER
}
