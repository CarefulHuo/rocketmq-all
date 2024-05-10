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
package org.apache.rocketmq.broker.transaction;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * 查找消息的结果
 */
public class OperationResult {

    /**
     * 消息
     */
    private MessageExt prepareMessage;

    /**
     * 查询结果的响应码
     */
    private int responseCode;

    private String responseRemark;

    public MessageExt getPrepareMessage() {
        return prepareMessage;
    }

    public void setPrepareMessage(MessageExt prepareMessage) {
        this.prepareMessage = prepareMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public String getResponseRemark() {
        return responseRemark;
    }

    public void setResponseRemark(String responseRemark) {
        this.responseRemark = responseRemark;
    }
}
