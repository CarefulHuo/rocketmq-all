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
package org.apache.rocketmq.store;

import java.util.Map;

/**
 * CommitLog 派发请求
 */
public class DispatchRequest {
    /**
     * 消息主题名称
     */
    private final String topic;
    /**
     * 消息队列ID
     */
    private final int queueId;
    /**
     * 消息在CommitLog中的物理偏移量
     */
    private final long commitLogOffset;

    /**
     * 消息大小/长度
     */
    private int msgSize;

    /**
     * 消息过滤 tag 哈希码，如果是延时消息，则为投递时间
     */
    private final long tagsCode;

    /**
     * 消息存储时间戳
     */
    private final long storeTimestamp;

    /**
     * 消息在消息消费队列的逻辑偏移量
     * @see CommitLog.DefaultAppendMessageCallback#doAppend(long, java.nio.ByteBuffer, int, org.apache.rocketmq.store.MessageExtBrokerInner)
     */
    private final long consumeQueueOffset;

    /**
     * 消息索引key，多个索引key以空格隔开，如：key1 key2
     * 存放在消息属性中的 keys：PROPERTY_KEYS = "KEYS"
     */
    private final String keys;

    /**
     * 是否成功解析到完整的消息
     */
    private final boolean success;

    /**
     * 消息唯一键
     */
    private final String uniqKey;

    /**
     * 消息系统标志
     */
    private final int sysFlag;

    /**
     * 消息预处理事务偏移量
     */
    private final long preparedTransactionOffset;

    /**
     * 消息属性
     */
    private final Map<String, String> propertiesMap;

    /**
     * 位图
     */
    private byte[] bitMap;

    private int bufferSize = -1;//the buffer size maybe larger than the msg size if the message is wrapped by something

    /**
     * @param topic                     消息主题名称
     * @param queueId                   消息消费队列Id
     * @param commitLogOffset           消息在CommitLog中的物理偏移量
     * @param msgSize                   消息大小
     * @param tagsCode                  消息的tagsCode，是延时消息时，存储的是投递时间
     * @param storeTimestamp            消息存储时间
     * @param consumeQueueOffset        消息在ConsumeQueue中的逻辑偏移量
     * @param keys                      附加属性中-提供一个或一组关键字，用于消息的索引或去重功能
     * @param uniqKey                   消息唯一键
     * @param sysFlag                   消息系统标记，包含消息处理的系统级别标志位，如是否是延迟消息，是否事务消息等信息
     * @param preparedTransactionOffset 消息预处理事务偏移量，仅当消息属于事务消息时使用，表示事务消息预提交状态的偏移量
     * @param propertiesMap             消息附加属性
     */
    public DispatchRequest(
            final String topic, // 消息主题名称
            final int queueId,  // 消息消费队列Id
            final long commitLogOffset, // 消息在CommitLog中的物理偏移量
            final int msgSize, // 消息大小
            final long tagsCode, // 消息的tagsCode，延时消息时，存储的是投递时间
            final long storeTimestamp, // 消息存储时间
            final long consumeQueueOffset,// 消息在ConsumeQueue中的逻辑偏移量
            final String keys, // 附加属性中-提供一个或一组关键字，用于消息的索引或去重功能
            final String uniqKey, // 附加属性-msgId 于确保消息的全局唯一性，或是作为消息去重的依据。
            final int sysFlag, // 消息系统标记，包含消息处理的系统级别标志位，如是否是延迟消息，是否事务消息等信息
            final long preparedTransactionOffset, // 预处理事务偏移量，仅当消息属于事务消息时使用，表示事务消息预提交状态的偏移量
            final Map<String, String> propertiesMap // 消息附加属性
    ) {
        //以下两个参数，确定是哪个 ConsumeQueue
        this.topic = topic;
        this.queueId = queueId;

        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;
        this.uniqKey = uniqKey;

        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(int size) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }

    public String getKeys() {
        return keys;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public byte[] getBitMap() {
        return bitMap;
    }

    public void setBitMap(byte[] bitMap) {
        this.bitMap = bitMap;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
