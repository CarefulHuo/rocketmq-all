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

import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.OperationResult;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 事务消息服务实现
 */
public class TransactionalMessageServiceImpl implements TransactionalMessageService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    /**
     * 事务消息辅助类
     */
    private TransactionalMessageBridge transactionalMessageBridge;

    private static final int PULL_MSG_RETRY_NUMBER = 1;

    private static final int MAX_PROCESS_TIME_LIMIT = 60000;

    private static final int MAX_RETRY_COUNT_WHEN_HALF_NULL = 1;

    public TransactionalMessageServiceImpl(TransactionalMessageBridge transactionBridge) {
        this.transactionalMessageBridge = transactionBridge;
    }

    /**
     * 消息队列对应的 OP 队列
     */
    private ConcurrentHashMap<MessageQueue, MessageQueue> opQueueMap = new ConcurrentHashMap<>();

    /**
     * 异步放入消息
     * @param messageInner Prepare(Half) message.
     * @return
     */
    @Override
    public CompletableFuture<PutMessageResult> asyncPrepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.asyncPutHalfMessage(messageInner);
    }

    /**
     * 同步放入消息
     * @param messageInner Prepare(Half) message.
     * @return
     */
    @Override
    public PutMessageResult prepareMessage(MessageExtBrokerInner messageInner) {
        return transactionalMessageBridge.putHalfMessage(messageInner);
    }

    /**
     * 是否丢弃事务消息，即事务消息提交失败，不能被消费者消费
     * @param msgExt
     * @param transactionCheckMax
     * @return
     */
    private boolean needDiscard(MessageExt msgExt, int transactionCheckMax) {
        // 获取消息当前 check 次数
        String checkTimes = msgExt.getProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES);
        int checkTime = 1;
        if (null != checkTimes) {
            checkTime = getInt(checkTimes);
            if (checkTime >= transactionCheckMax) {
                // 达到最大检查次数，默认 15 次，则跳过
                return true;
            } else {
                // 没有达到，则更新当前消息的 check 次数
                checkTime++;
            }
        }

        // 消息属性：TRANSACTION_CHECK_TIMES 增加 1
        msgExt.putUserProperty(MessageConst.PROPERTY_TRANSACTION_CHECK_TIMES, String.valueOf(checkTime));
        return false;
    }

    /**
     * 是否需要跳过
     * @param msgExt 消息
     * @return
     */
    private boolean needSkip(MessageExt msgExt) {
        // 获取消息产生到现在过了多久
        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();

        // 如果消息超时，超过 72 h 则跳过
        if (valueOfCurrentMinusBorn
            > transactionalMessageBridge.getBrokerController().getMessageStoreConfig().getFileReservedTime()
            * 3600L * 1000) {
            log.info("Half message exceed file reserved time ,so skip it.messageId {},bornTime {}",
                msgExt.getMsgId(), msgExt.getBornTimestamp());
            return true;
        }
        return false;
    }

    /**
     * 重新添加半消息
     *
     * @param msgExt 半消息
     * @param offset 半消息在队列中的逻辑偏移量
     * @return
     */
    private boolean putBackHalfMsgQueue(MessageExt msgExt, long offset) {
        // 重新写入半消息
        PutMessageResult putMessageResult = putBackToHalfQueueReturnResult(msgExt);

        // 写入半消息成功
        if (putMessageResult != null && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
            // 更新消息队列逻辑偏移量
            msgExt.setQueueOffset(
                putMessageResult.getAppendMessageResult().getLogicsOffset());

            // 更新消息队列物理偏移量
            msgExt.setCommitLogOffset(
                putMessageResult.getAppendMessageResult().getWroteOffset());

            // 更新消息Id
            msgExt.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            log.debug(
                "Send check message, the offset={} restored in queueOffset={} "
                    + "commitLogOffset={} "
                    + "newMsgId={} realMsgId={} topic={}",
                offset, msgExt.getQueueOffset(), msgExt.getCommitLogOffset(), msgExt.getMsgId(),
                msgExt.getUserProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
                msgExt.getTopic());
            return true;
        } else {
            log.error(
                "PutBackToHalfQueueReturnResult write failed, topic: {}, queueId: {}, "
                    + "msgId: {}",
                msgExt.getTopic(), msgExt.getQueueId(), msgExt.getMsgId());
            return false;
        }
    }

    /**
     * 事务消息回查
     * todo 总的来说
     * 1.一条一条拉取(消费进度)，如果在 op 队列，就是已经 commit 或者 rollback 的，不用再管了，否则就需要检查是否需要回查，需要的话，这条消息需要写回 half 队列
     * 2.从 op 队列中确认事务状态，是根据 op 队列里拉取的消息的消息体(保存的是事务半消息的逻辑偏移量) 来判断当前偏移的事务消息是否已经处理过了
     *
     * @param transactionTimeout 检查事务消息的最小时间，只有一条消息超过了可以检查的时间间隔
     * The minimum time of the transactional message to be checked firstly, one message only exceed this time interval that can be checked.
     * @param transactionCheckMax 最大回查次数
     * The maximum number of times the message was checked, if exceed this value, this message will be discarded.
     * @param listener 当消息被检查过或丢弃时，调用该监听器的 relative 方法
     * When the message is considered to be checked or discarded, the relative method of this class will be invoked.
     */
    @Override
    public void check(long transactionTimeout, int transactionCheckMax,
        AbstractTransactionalMessageCheckListener listener) {
        try {
            // 事务消息主题，这个是固定的
            String topic = TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;

            // 根据事务消息主题获取 MessageQueue，这个也是固定一个，队列Id：0
            Set<MessageQueue> msgQueues = transactionalMessageBridge.fetchMessageQueues(topic);
            if (msgQueues == null || msgQueues.size() == 0) {
                log.warn("The queue of topic is empty :" + topic);
                return;
            }
            log.debug("Check topic={}, queues={}", topic, msgQueues);
            // 遍历 Half MessageQueue ，其实就一个队列，具体逻辑如下：
            // 将 Half 消息与 op 消息对比，如果 op 中包含该消息，则不回查，
            // 如果不包含，并且 Half 中的该消息存储时间超过了限制时间或者最后一条 op 的存储时间超过了事务超时时间，则进行回查
            for (MessageQueue messageQueue : msgQueues) {
                // 记录开始时间
                long startTime = System.currentTimeMillis();

                // todo 根据消息队列获取对应的 op 队列，没有则新建一个
                MessageQueue opQueue = getOpQueue(messageQueue);

                // todo 获取 Half 消息队列的当前消费进度(逻辑偏移量)，也就是已经确定的消费进度，op 消息会根据该确定进度判断哪些 Half 消息已经确定了，然后收集起来
                // todo Half 消息队列消费进度，只会在回查的时候更新，因此正常情况下 Half 消息的消费进度时要小于 OP 消息存储的消费进度的
                long halfOffset = transactionalMessageBridge.fetchConsumeOffset(messageQueue);

                // 获取 OP 消息队列的当前消费进度(逻辑偏移量)
                // todo op 消息队列消费进度，只会在回查的时候更新
                long opOffset = transactionalMessageBridge.fetchConsumeOffset(opQueue);
                log.info("Before check, the queue={} msgOffset={} opOffset={}", messageQueue, halfOffset, opOffset);
                if (halfOffset < 0 || opOffset < 0) {
                    log.error("MessageQueue: {} illegal offset read: {}, op offset: {},skip this queue", messageQueue,
                        halfOffset, opOffset);
                    continue;
                }

                // OP 队列的消费 offset
                List<Long> doneOpOffset = new ArrayList<>();

                // todo removeMap 表示哪些 Half(通过当前 Half 消息逻辑偏移量 offset 作为参考) 已经完成不需要再回查了(half 消息中已经存在对应的 OP 消息了)
                // key：已经完成的 Half 消息逻辑偏移量 offset (在拉取一批 OP 消息后判断其存储的已经确定的 Half 消息逻辑偏移量 >= 已经确定的消费进度 HalfOffset)，在推进 Half 消息的过程就可以前置过滤了，避免重复回查
                // value：OP消息逻辑偏移量 Offset
                HashMap<Long, Long> removeMap = new HashMap<>();

                /**
                 * 1. 从 opValue 队列中拉取 op 消息，如果 op 消息中记录的消息进度 >= halfOffset (half消息进度)，说明 HalfOffset 对应的 Half 消息则不需要回查
                 *    也就是该方法批量拉取 op 消息，然后基于 halfOffset 判断哪些消息进度的 half 消息不需要回查，因为这些 half 消息已经有了 op 消息了，不需要再回查了，
                 *    并把不需要回查的 half消息的进度(halfOffset)以及保存该进度的 op消息的进度(opOffset)添加到 removeMap 中
                 * 2. 基于 halfOffset 判断的仍需要回查的当前的 op 消息的进度放入到 doneOpOffset 中
                 * 3. 返回 pullRequest 是批量拉取的 op 消息
                 * todo 为了避免重复调用事务回查接口，前置将已经处理好的 half 消息，使用 op 消息过滤掉，过滤的条件是根据当前 half 队列的逻辑偏移量
                 * 只要 op 消息存储的逻辑偏移量 >= 该偏移量，就说明该 half 偏移量的 half 消息已经被处理了，从这里看就会过滤 halfOffset 这一个逻辑偏移，
                 * 但是从后面可以知道，在向消息生产回查之前，该 half 消息会被重新放入存储的
                 */
                // 找出需要校验是否需要回查的记录中已经 commit 或 rollback 的消息存放到 removeMap
                PullResult pullResult = fillOpRemoveMap(removeMap, opQueue, opOffset, halfOffset, doneOpOffset);
                if (null == pullResult) {
                    log.error("The queue={} check msgOffset={} with opOffset={} failed, pullResult is null",
                        messageQueue, halfOffset, opOffset);
                    continue;
                }
                // single thread
                int getMessageNullCount = 1;
                long newOffset = halfOffset;
                long i = halfOffset;
                while (true) {
                    if (System.currentTimeMillis() - startTime > MAX_PROCESS_TIME_LIMIT) {
                        log.info("Queue={} process time reach max={}", messageQueue, MAX_PROCESS_TIME_LIMIT);
                        break;
                    }
                    if (removeMap.containsKey(i)) {
                        log.debug("Half offset {} has been committed/rolled back", i);
                        Long removedOpOffset = removeMap.remove(i);
                        doneOpOffset.add(removedOpOffset);
                    } else {
                        GetResult getResult = getHalfMsg(messageQueue, i);
                        MessageExt msgExt = getResult.getMsg();
                        if (msgExt == null) {
                            if (getMessageNullCount++ > MAX_RETRY_COUNT_WHEN_HALF_NULL) {
                                break;
                            }
                            if (getResult.getPullResult().getPullStatus() == PullStatus.NO_NEW_MSG) {
                                log.debug("No new msg, the miss offset={} in={}, continue check={}, pull result={}", i,
                                    messageQueue, getMessageNullCount, getResult.getPullResult());
                                break;
                            } else {
                                log.info("Illegal offset, the miss offset={} in={}, continue check={}, pull result={}",
                                    i, messageQueue, getMessageNullCount, getResult.getPullResult());
                                i = getResult.getPullResult().getNextBeginOffset();
                                newOffset = i;
                                continue;
                            }
                        }

                        if (needDiscard(msgExt, transactionCheckMax) || needSkip(msgExt)) {
                            listener.resolveDiscardMsg(msgExt);
                            newOffset = i + 1;
                            i++;
                            continue;
                        }
                        if (msgExt.getStoreTimestamp() >= startTime) {
                            log.debug("Fresh stored. the miss offset={}, check it later, store={}", i,
                                new Date(msgExt.getStoreTimestamp()));
                            break;
                        }

                        long valueOfCurrentMinusBorn = System.currentTimeMillis() - msgExt.getBornTimestamp();
                        long checkImmunityTime = transactionTimeout;
                        String checkImmunityTimeStr = msgExt.getUserProperty(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
                        if (null != checkImmunityTimeStr) {
                            checkImmunityTime = getImmunityTime(checkImmunityTimeStr, transactionTimeout);
                            if (valueOfCurrentMinusBorn < checkImmunityTime) {
                                if (checkPrepareQueueOffset(removeMap, doneOpOffset, msgExt)) {
                                    newOffset = i + 1;
                                    i++;
                                    continue;
                                }
                            }
                        } else {
                            if ((0 <= valueOfCurrentMinusBorn) && (valueOfCurrentMinusBorn < checkImmunityTime)) {
                                log.debug("New arrived, the miss offset={}, check it later checkImmunity={}, born={}", i,
                                    checkImmunityTime, new Date(msgExt.getBornTimestamp()));
                                break;
                            }
                        }
                        List<MessageExt> opMsg = pullResult.getMsgFoundList();
                        boolean isNeedCheck = (opMsg == null && valueOfCurrentMinusBorn > checkImmunityTime)
                            || (opMsg != null && (opMsg.get(opMsg.size() - 1).getBornTimestamp() - startTime > transactionTimeout))
                            || (valueOfCurrentMinusBorn <= -1);

                        if (isNeedCheck) {
                            if (!putBackHalfMsgQueue(msgExt, i)) {
                                continue;
                            }
                            listener.resolveHalfMsg(msgExt);
                        } else {
                            pullResult = fillOpRemoveMap(removeMap, opQueue, pullResult.getNextBeginOffset(), halfOffset, doneOpOffset);
                            log.debug("The miss offset:{} in messageQueue:{} need to get more opMsg, result is:{}", i,
                                messageQueue, pullResult);
                            continue;
                        }
                    }
                    newOffset = i + 1;
                    i++;
                }
                if (newOffset != halfOffset) {
                    transactionalMessageBridge.updateConsumeOffset(messageQueue, newOffset);
                }
                long newOpOffset = calculateOpOffset(doneOpOffset, opOffset);
                if (newOpOffset != opOffset) {
                    transactionalMessageBridge.updateConsumeOffset(opQueue, newOpOffset);
                }
            }
        } catch (Throwable e) {
            log.error("Check error", e);
        }

    }

    private long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime;

        checkImmunityTime = getLong(checkImmunityTimeStr);
        if (-1 == checkImmunityTime) {
            checkImmunityTime = transactionTimeout;
        } else {
            checkImmunityTime *= 1000;
        }
        return checkImmunityTime;
    }

    /**
     * 读取 op 消息、解析 op 消息并填充 removeMap
     * Read op message, parse op message, and fill removeMap
     *
     * @param removeMap Half message to be remove, key:halfOffset, value: opOffset. 要删除的半消息，键：halfOffset，值：opOffset。
     * @param opQueue Op message queue. op 消息队列
     * @param pullOffsetOfOp The begin offset of op message queue. op 消息队列的开始偏移量。
     * @param miniOffset The current minimum offset of half message queue. 消息队列当前最小的偏移量
     * @param doneOpOffset Stored op messages that have been processed. 已处理的已存储操作消息。
     * @return Op message result.
     */
    private PullResult fillOpRemoveMap(HashMap<Long, Long> removeMap,
        MessageQueue opQueue, long pullOffsetOfOp, long miniOffset, List<Long> doneOpOffset) {

        // 从 op 队列中读取一定数量的消息，默认最大 32 条
        PullResult pullResult = pullOpMsg(opQueue, pullOffsetOfOp, 32);
        if (null == pullResult) {
            return null;
        }
        if (pullResult.getPullStatus() == PullStatus.OFFSET_ILLEGAL
            || pullResult.getPullStatus() == PullStatus.NO_MATCHED_MSG) {
            log.warn("The miss op offset={} in queue={} is illegal, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            transactionalMessageBridge.updateConsumeOffset(opQueue, pullResult.getNextBeginOffset());
            return pullResult;
        } else if (pullResult.getPullStatus() == PullStatus.NO_NEW_MSG) {
            log.warn("The miss op offset={} in queue={} is NO_NEW_MSG, pullResult={}", pullOffsetOfOp, opQueue,
                pullResult);
            return pullResult;
        }
        List<MessageExt> opMsg = pullResult.getMsgFoundList();
        if (opMsg == null) {
            log.warn("The miss op offset={} in queue={} is empty, pullResult={}", pullOffsetOfOp, opQueue, pullResult);
            return pullResult;
        }

        // 遍历拉取到的 op 队列中的消息，判断一下这些 op 消息对应的 half 消息是否处理过了(基于传入的 half 队列的消费进度 miniOffset)
        for (MessageExt opMessageExt : opMsg) {

            // op 队列中存储的内容是 half 队列事务消息已经 Commit 和 rollback 的消息的逻辑偏移量 Offset
            Long queueOffset = getLong(new String(opMessageExt.getBody(), TransactionalMessageUtil.charset));

            log.debug("Topic: {} tags: {}, OpOffset: {}, HalfOffset: {}", opMessageExt.getTopic(),
                opMessageExt.getTags(), opMessageExt.getQueueOffset(), queueOffset);

            // 如果该消息是个删除操作
            if (TransactionalMessageUtil.REMOVETAG.equals(opMessageExt.getTags())) {
                if (queueOffset < miniOffset) {
                    // 消息进度小于消息队列消费进度，需要校验，则重新添加
                    doneOpOffset.add(opMessageExt.getQueueOffset());
                } else {
                    // todo 不需要校验，存储已经完成的 half 消息的逻辑偏移量到 op 消息的逻辑偏移量
                    removeMap.put(queueOffset, opMessageExt.getQueueOffset());
                }
            } else {
                log.error("Found a illegal tag in opMessageExt= {} ", opMessageExt);
            }
        }
        log.debug("Remove map: {}", removeMap);
        log.debug("Done op list: {}", doneOpOffset);
        return pullResult;
    }

    /**
     * If return true, skip this msg
     *
     * @param removeMap Op message map to determine whether a half message was responded by producer.
     * @param doneOpOffset Op Message which has been checked.
     * @param msgExt Half message
     * @return Return true if put success, otherwise return false.
     */
    private boolean checkPrepareQueueOffset(HashMap<Long, Long> removeMap, List<Long> doneOpOffset,
        MessageExt msgExt) {
        String prepareQueueOffsetStr = msgExt.getUserProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        if (null == prepareQueueOffsetStr) {
            return putImmunityMsgBackToHalfQueue(msgExt);
        } else {
            long prepareQueueOffset = getLong(prepareQueueOffsetStr);
            if (-1 == prepareQueueOffset) {
                return false;
            } else {
                if (removeMap.containsKey(prepareQueueOffset)) {
                    long tmpOpOffset = removeMap.remove(prepareQueueOffset);
                    doneOpOffset.add(tmpOpOffset);
                    return true;
                } else {
                    return putImmunityMsgBackToHalfQueue(msgExt);
                }
            }
        }
    }

    /**
     * Write messageExt to Half topic again
     *
     * @param messageExt Message will be write back to queue
     * @return Put result can used to determine the specific results of storage.
     */
    private PutMessageResult putBackToHalfQueueReturnResult(MessageExt messageExt) {
        PutMessageResult putMessageResult = null;
        try {
            MessageExtBrokerInner msgInner = transactionalMessageBridge.renewHalfMessageInner(messageExt);
            putMessageResult = transactionalMessageBridge.putMessageReturnResult(msgInner);
        } catch (Exception e) {
            log.warn("PutBackToHalfQueueReturnResult error", e);
        }
        return putMessageResult;
    }

    private boolean putImmunityMsgBackToHalfQueue(MessageExt messageExt) {
        MessageExtBrokerInner msgInner = transactionalMessageBridge.renewImmunityHalfMessageInner(messageExt);
        return transactionalMessageBridge.putMessage(msgInner);
    }

    /**
     * 从 half 主题中拉取 half 消息
     * Read half message from Half Topic
     *
     * @param mq Target message queue, in this method, it means the half message queue.
     * @param offset Offset in the message queue.
     * @param nums Pull message number.
     * @return Messages pulled from half message queue.
     */
    private PullResult pullHalfMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getHalfMessage(mq.getQueueId(), offset, nums);
    }

    /**
     * 从 op 主题读取 op 队列中的消息，默认最大 32 条
     * Read op message from Op Topic
     *
     * @param mq Target Message Queue
     * @param offset Offset in the message queue
     * @param nums Pull message number
     * @return Messages pulled from operate message queue.
     */
    private PullResult pullOpMsg(MessageQueue mq, long offset, int nums) {
        return transactionalMessageBridge.getOpMessage(mq.getQueueId(), offset, nums);
    }

    private Long getLong(String s) {
        long v = -1;
        try {
            v = Long.valueOf(s);
        } catch (Exception e) {
            log.error("GetLong error", e);
        }
        return v;

    }

    private Integer getInt(String s) {
        int v = -1;
        try {
            v = Integer.valueOf(s);
        } catch (Exception e) {
            log.error("GetInt error", e);
        }
        return v;

    }

    private long calculateOpOffset(List<Long> doneOffset, long oldOffset) {
        Collections.sort(doneOffset);
        long newOffset = oldOffset;
        for (int i = 0; i < doneOffset.size(); i++) {
            if (doneOffset.get(i) == newOffset) {
                newOffset++;
            } else {
                break;
            }
        }
        return newOffset;

    }

    /**
     * 根据消息队列获取对应的 op 消息队列
     * @param messageQueue
     * @return
     */
    private MessageQueue getOpQueue(MessageQueue messageQueue) {
        // 根据消息队列获取对应的 OP 队列
        MessageQueue opQueue = opQueueMap.get(messageQueue);

        // 如果对应的 OP 队列为空 则创建一个对应的 OP 队列
        if (opQueue == null) {
            // 主题：RMQ_SYS_TRANS_OP_HALF_TOPIC
            // BrokerName：同消息队列
            // QueueId：同消息队列
            opQueue = new MessageQueue(TransactionalMessageUtil.buildOpTopic(), messageQueue.getBrokerName(),
                messageQueue.getQueueId());
            opQueueMap.put(messageQueue, opQueue);
        }
        return opQueue;

    }

    /**
     * 拉取半消息
     *
     * @param messageQueue 消息队列
     * @param offset       偏移量
     * @return
     */
    private GetResult getHalfMsg(MessageQueue messageQueue, long offset) {
        GetResult getResult = new GetResult();

        // 拉取半消息
        PullResult result = pullHalfMsg(messageQueue, offset, PULL_MSG_RETRY_NUMBER);
        getResult.setPullResult(result);
        List<MessageExt> messageExts = result.getMsgFoundList();
        if (messageExts == null) {
            return getResult;
        }
        getResult.setMsg(messageExts.get(0));
        return getResult;
    }

    private OperationResult getHalfMessageByOffset(long commitLogOffset) {
        OperationResult response = new OperationResult();
        // 根据物理偏移量获取消息
        MessageExt messageExt = this.transactionalMessageBridge.lookMessageByOffset(commitLogOffset);
        if (messageExt != null) {
            response.setPrepareMessage(messageExt);
            response.setResponseCode(ResponseCode.SUCCESS);
        } else {
            response.setResponseCode(ResponseCode.SYSTEM_ERROR);
            response.setResponseRemark("Find prepared transaction message failed");
        }
        return response;
    }

    @Override
    public boolean deletePrepareMessage(MessageExt msgExt) {
        // 存储op消息，将 op消息 存储到 RMQ_SYS_TRANS_OP_HALF_TOPIC 主题下，表示该事务消息已经处理了(包括提交和回滚)
        if (this.transactionalMessageBridge.putOpMessage(msgExt, TransactionalMessageUtil.REMOVETAG)) {
            log.debug("Transaction op message write successfully. messageId={}, queueId={} msgExt:{}", msgExt.getMsgId(), msgExt.getQueueId(), msgExt);
            return true;
        } else {
            log.error("Transaction op message write failed. messageId is {}, queueId is {}", msgExt.getMsgId(), msgExt.getQueueId());
            return false;
        }
    }

    /**
     * 根据消息物理偏移量查找消息，这个过程有两步
     * 1. 先根据物理偏移量读取 4 个字节的内容，该内容是一个消息的大小 size
     * 2. 根据物理偏移量读取 size 大小的内容，此时就是消息
     * @param requestHeader Commit message request header.
     * @return
     */
    @Override
    public OperationResult commitMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    /**
     * 根据消息物理偏移量查询消息，这个过程有两步
     * 1. 先根据物理偏移量读取 4 个字节的内容，该内容就是一个消息的大小 size
     * 2. 根据物理偏移量读取 size 大小的内容，此时就是消息内容
     * @param requestHeader Prepare message request header.
     * @return
     */
    @Override
    public OperationResult rollbackMessage(EndTransactionRequestHeader requestHeader) {
        return getHalfMessageByOffset(requestHeader.getCommitLogOffset());
    }

    @Override
    public boolean open() {
        return true;
    }

    @Override
    public void close() {

    }

}
