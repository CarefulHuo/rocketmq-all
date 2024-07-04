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

package org.apache.rocketmq.client.common;

/**
 * 客户错误码
 */
public class ClientErrorCode {

    /**
     * 连接Broker异常
     */
    public static final int CONNECT_BROKER_EXCEPTION = 10001;

    /**
     * 访问Broker超时
     */
    public static final int ACCESS_BROKER_TIMEOUT = 10002;

    /**
     * Broker不存在异常
     */
    public static final int BROKER_NOT_EXIST_EXCEPTION = 10003;

    /**
     * 没有 NameServer 异常
     */
    public static final int NO_NAME_SERVER_EXCEPTION = 10004;

    /**
     * 没有找到 Topic 异常
     */
    public static final int NOT_FOUND_TOPIC_EXCEPTION = 10005;

    /**
     * 请求超时异常
     */
    public static final int REQUEST_TIMEOUT_EXCEPTION = 10006;

    /**
     * 创建回复消息异常
     */
    public static final int CREATE_REPLY_MESSAGE_EXCEPTION = 10007;
}