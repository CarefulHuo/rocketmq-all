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

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * 消息扩展类
 */
public class MessageExt extends Message {
    private static final long serialVersionUID = 5720810158625748049L;

    /**
     * 消息存储的 Broker 名称
     */
    private String brokerName;

    /**
     * 消息存储的队列编号(MessageQueue 序号)
     */
    private int queueId;

    /**
     * 消息存储大小，单位：字节
     */
    private int storeSize;

    /**
     * 消息在队列中的偏移量，实际是 ConsumeQueue 的逻辑偏移量
     */
    private long queueOffset;

    /**
     * 系统标志，什么类型的消息
     */
    private int sysFlag;

    /**
     * 消息的创建时间
     */
    private long bornTimestamp;
    private SocketAddress bornHost;

    /**
     * 消息存储时间
     */
    private long storeTimestamp;

    /**
     * 连接到的 Broker 的 Socket
     */
    private SocketAddress storeHost;

    /**
     * todo 本质上是 OffsetMsgId，是从 Broker 拉取消息之后，生成的一个Id，并非我们所说的那个 msgId
     *  注意：
     *   1. 该属性值是客户端在解析消息时生成的，Broker 端没有存储。
     *   @see org.apache.rocketmq.client.impl.consumer.PullAPIWrapper#processPullResult(MessageQueue, org.apache.rocketmq.client.consumer.PullResult, SubscriptionData)
     *   2. 当前类的 {@link MessageExt#toString()}  方法打印的是 msgId ，也就是 OffsetMsgId
     *   3. {@link MessageExt#getMsgId()} 方法返回的却是 msgId，因为子类重写该方法
     */
    private String msgId;

    /**
     * 在 CommitLog 中的物理偏移量
     */
    private long commitLogOffset;
    private int bodyCRC;

    /**
     * 重新消费次数，第一次消息消费时为 0
     */
    private int reconsumeTimes;

    private long preparedTransactionOffset;

    public MessageExt() {
    }

    public MessageExt(int queueId, long bornTimestamp, SocketAddress bornHost, long storeTimestamp,
        SocketAddress storeHost, String msgId) {
        this.queueId = queueId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeTimestamp = storeTimestamp;
        this.storeHost = storeHost;
        this.msgId = msgId;
    }

    public static TopicFilterType parseTopicFilterType(final int sysFlag) {
        if ((sysFlag & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG) {
            return TopicFilterType.MULTI_TAG;
        }

        return TopicFilterType.SINGLE_TAG;
    }

    public static ByteBuffer socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        if (address instanceof Inet4Address) {
            byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
        } else {
            byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 16);
        }
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.flip();
        return byteBuffer;
    }

    public static ByteBuffer socketAddress2ByteBuffer(SocketAddress socketAddress) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        ByteBuffer byteBuffer;
        if (address instanceof Inet4Address) {
            byteBuffer = ByteBuffer.allocate(4 + 4);
        } else {
            byteBuffer = ByteBuffer.allocate(16 + 4);
        }
        return socketAddress2ByteBuffer(socketAddress, byteBuffer);
    }

    public ByteBuffer getBornHostBytes() {
        return socketAddress2ByteBuffer(this.bornHost);
    }

    public ByteBuffer getBornHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.bornHost, byteBuffer);
    }

    public ByteBuffer getStoreHostBytes() {
        return socketAddress2ByteBuffer(this.storeHost);
    }

    public ByteBuffer getStoreHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.storeHost, byteBuffer);
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    public void setBornTimestamp(long bornTimestamp) {
        this.bornTimestamp = bornTimestamp;
    }

    public SocketAddress getBornHost() {
        return bornHost;
    }

    public void setBornHost(SocketAddress bornHost) {
        this.bornHost = bornHost;
    }

    public String getBornHostString() {
        if (null != this.bornHost) {
            InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();

            return null != inetAddress ? inetAddress.getHostAddress() : null;
        }

        return null;
    }

    public String getBornHostNameString() {
        if (null != this.bornHost) {
            InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();

            return null != inetAddress ? inetAddress.getHostName() : null;
        }

        return null;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(SocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    /**
     * todo 子类重写了该方法，返回的不再是 OffsetMsgId
     * @return
     */
    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public void setStoreHostAddressV6Flag() { this.sysFlag = this.sysFlag | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG; }

    public void setBornHostV6Flag() { this.sysFlag = this.sysFlag | MessageSysFlag.BORNHOST_V6_FLAG; }

    public int getBodyCRC() {
        return bodyCRC;
    }

    public void setBodyCRC(int bodyCRC) {
        this.bodyCRC = bodyCRC;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(long physicOffset) {
        this.commitLogOffset = physicOffset;
    }

    public int getStoreSize() {
        return storeSize;
    }

    public void setStoreSize(int storeSize) {
        this.storeSize = storeSize;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public void setPreparedTransactionOffset(long preparedTransactionOffset) {
        this.preparedTransactionOffset = preparedTransactionOffset;
    }

    @Override
    public String toString() {
        return "MessageExt [brokerName=" + brokerName + ", queueId=" + queueId + ", storeSize=" + storeSize + ", queueOffset=" + queueOffset
            + ", sysFlag=" + sysFlag + ", bornTimestamp=" + bornTimestamp + ", bornHost=" + bornHost
            + ", storeTimestamp=" + storeTimestamp + ", storeHost=" + storeHost + ", msgId=" + msgId
            + ", commitLogOffset=" + commitLogOffset + ", bodyCRC=" + bodyCRC + ", reconsumeTimes="
            + reconsumeTimes + ", preparedTransactionOffset=" + preparedTransactionOffset
            + ", toString()=" + super.toString() + "]";
    }
}
