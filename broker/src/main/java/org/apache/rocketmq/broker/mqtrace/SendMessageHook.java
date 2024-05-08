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
 * 定义了一个发送消息的钩子（SendMessageHook），允许在发送消息之前和之后执行自定义的操作。
 * hookName()方法返回钩子的名称，用于标识具体的钩子实现。
 * sendMessageBefore(final SendMessageContext context)方法在发送消息之前被调用，传入一个SendMessageContext对象，其中可能包含了发送消息的上下文信息，如消息内容、发送者等。该方法可以用于执行发送消息前的准备工作，如日志记录、权限验证等。
 * sendMessageAfter(final SendMessageContext context)方法在发送消息之后被调用，同样传入一个SendMessageContext对象。该方法可以用于执行发送消息后的操作，如记录发送结果、发送回执等。
 * 实现该接口的类可以根据具体需求，自定义hookName()方法的返回值，并在sendMessageBefore()和sendMessageAfter()方法中实现自定义的逻辑。
 */
public interface SendMessageHook {
    String hookName();

    void sendMessageBefore(final SendMessageContext context);

    void sendMessageAfter(final SendMessageContext context);
}
