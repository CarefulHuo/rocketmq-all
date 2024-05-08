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
package org.apache.rocketmq.broker.mqtrace;

/**
 * 定义了一个消息消费的钩子，允许在消费消息之前和之后执行自定义的操作。
 * hookName()方法返回钩子的名称，用于标识不同的钩子。
 * consumeMessageBefore(ConsumeMessageContext context)方法在消费消息之前被调用，传入一个包含消息上下文的参数，可以在该方法中进行一些预处理或日志记录。
 * consumeMessageAfter(ConsumeMessageContext context)方法在消费消息之后被调用，传入相同的上下文参数，可以在该方法中进行一些后处理操作，如记录消费耗时或处理结果。
 * 使用该接口的实现可以对消息消费过程进行扩展和定制。
 */
public interface ConsumeMessageHook {
    String hookName();

    void consumeMessageBefore(final ConsumeMessageContext context);

    void consumeMessageAfter(final ConsumeMessageContext context);
}
