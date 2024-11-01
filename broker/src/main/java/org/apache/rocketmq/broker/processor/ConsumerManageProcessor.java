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
package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetResponseHeader;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class ConsumerManageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public ConsumerManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                // 获取某个消费组下所有的消费者id
                return this.getConsumerListByGroup(ctx, request);
            case RequestCode.UPDATE_CONSUMER_OFFSET:
                // 更新消费者进度，即 Consumer 上报自己的消费进度
                return this.updateConsumerOffset(ctx, request);
            case RequestCode.QUERY_CONSUMER_OFFSET:
                // 获取消费进度，即获取某个 Topic 下的某个消费组的某个队列的消费进度
                return this.queryConsumerOffset(ctx, request);
            default:
                break;
        }
        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 获取某个消费组下所有的消费者id
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    public RemotingCommand getConsumerListByGroup(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(GetConsumerListByGroupResponseHeader.class);
        // 解码请求
        final GetConsumerListByGroupRequestHeader requestHeader =
            (GetConsumerListByGroupRequestHeader) request
                .decodeCommandCustomHeader(GetConsumerListByGroupRequestHeader.class);

        // 根据消费组名-获取消费组信息
        ConsumerGroupInfo consumerGroupInfo =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(
                requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            // 获取消费组信息中的所有消费者id，并返回
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            if (!clientIds.isEmpty()) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(clientIds);
                response.setBody(body.encode());
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);
                return response;
            } else {
                log.warn("getAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            log.warn("getConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());
        return response;
    }

    /**
     * 更新消费者进度，即 Consumer 上报自己的消费进度
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand updateConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(UpdateConsumerOffsetResponseHeader.class);
        // 解码请求
        final UpdateConsumerOffsetRequestHeader requestHeader =
            (UpdateConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class);

        // 将某个 Topic 下的某个消费组下的某个队列的消费进度提交
        this.brokerController.getConsumerOffsetManager().commitOffset(
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                requestHeader.getConsumerGroup(),
                requestHeader.getTopic(),
                requestHeader.getQueueId(),
                requestHeader.getCommitOffset());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 查询消费进度，即获取某个 Topic 下的某个消费组的某个队列的消费进度
     * @param ctx
     * @param request
     * @return
     * @throws RemotingCommandException
     */
    private RemotingCommand queryConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {

        // 创建响应对象
        final RemotingCommand response =
            RemotingCommand.createResponseCommand(QueryConsumerOffsetResponseHeader.class);
        final QueryConsumerOffsetResponseHeader responseHeader =
            (QueryConsumerOffsetResponseHeader) response.readCustomHeader();

        // 解码请求
        final QueryConsumerOffsetRequestHeader requestHeader =
            (QueryConsumerOffsetRequestHeader) request
                .decodeCommandCustomHeader(QueryConsumerOffsetRequestHeader.class);

        // 根据 Topic 、 Group 、 消费队列id，获取该消费队列的消费进度
        // 没有该队列，返回 -1
        long offset =
            this.brokerController.getConsumerOffsetManager().queryOffset(
                requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());

        // 如果 Offset 大于 0 直接返回
        if (offset >= 0) {
            responseHeader.setOffset(offset);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            // 获取该消息主题、消费队列当前在 Broker 服务器上最小的逻辑偏移量
            // 如果返回 0 则表示该队列的文件还未删除
            long minOffset =
                this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(),
                    requestHeader.getQueueId());

            // 如果最小偏移量为 0 && 该偏移量对应的 CommitLog 仍然在内存中，而不是在磁盘中，则返回 0
            if (minOffset <= 0
                && !this.brokerController.getMessageStore().checkInDiskByConsumeOffset(
                requestHeader.getTopic(), requestHeader.getQueueId(), 0)) {
                responseHeader.setOffset(0L);
                response.setCode(ResponseCode.SUCCESS);
                response.setRemark(null);

                // 如果最小偏移量 大于0 或者 最小偏移量小于等于0 但是该偏移量对应的 CommitLog 已经在磁盘中，则返回 未找到，
                // 但是，最终 RebalancePushImpl#computePullFromWhere 中得到的偏移量为-1。
            } else {
                response.setCode(ResponseCode.QUERY_NOT_FOUND);
                response.setRemark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }

        return response;
    }
}
