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
package org.apache.rocketmq.client.impl.producer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureTable;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;

public class DefaultMQProducerImpl implements MQProducerInner {
    private final InternalLogger log = ClientLogger.getLog();
    private final Random random = new Random();
    private final DefaultMQProducer defaultMQProducer;

    /**
     * Producer 本地缓存的 Topic 信息表 -- 由路由信息转换而得到的 (todo 注意：起始的时候是全部的队列)
     * <p>
     * 根据 Topic 路由设置该字段的情况如下：
     *
     * @see MQClientInstance#updateTopicRouteInfoFromNameServer(java.lang.String, boolean, org.apache.rocketmq.client.producer.DefaultMQProducer)
     */
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
        new ConcurrentHashMap<String, TopicPublishInfo>();
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final ArrayList<EndTransactionHook> endTransactionHookList = new ArrayList<EndTransactionHook>();
    private final RPCHook rpcHook;
    private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;

    /**
     * 事务状态回查的线程池
     */
    private final ExecutorService defaultAsyncSenderExecutor;
    private final Timer timer = new Timer("RequestHouseKeepingService", true);
    protected BlockingQueue<Runnable> checkRequestQueue;
    protected ExecutorService checkExecutor;

    /**
     * 服务状态
     * todo RocketMQ 使用一个成员变量 ServiceState 来记录和管理自身的服务状态，这实际上是状态模式(state Pattern)的变种实现
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;

    /**
     * 客户端实例
     * todo 封装了客户端一些通用的业务逻辑，无论是 producer 还是 Consumer 最终需要跟服务端进行交互时，都需要调用这个类中的方法，
     */
    private MQClientInstance mQClientFactory;
    private ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<CheckForbiddenHook>();
    private int zipCompressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));

    /**
     * 消息发送容错策略
     */
    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
    private ExecutorService asyncSenderExecutor;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;

        this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<Runnable>(50000);
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.asyncSenderThreadPoolQueue,
            new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
                }
            });
    }

    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
        log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
            checkForbiddenHookList.size());
    }

    /**
     * 用于事务消息场景
     * 初始化事务环境
     */
    public void initTransactionEnv() {
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;

        // 设置事务状态回查线程池
        if (producer.getExecutorService() != null) {
            this.checkExecutor = producer.getExecutorService();
        } else {
            this.checkRequestQueue = new LinkedBlockingQueue<Runnable>(producer.getCheckRequestHoldMax());
            this.checkExecutor = new ThreadPoolExecutor(
                producer.getCheckThreadPoolMinSize(),
                producer.getCheckThreadPoolMaxSize(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.checkRequestQueue);
        }
    }

    public void destroyTransactionEnv() {
        if (this.checkExecutor != null) {
            this.checkExecutor.shutdown();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register sendMessage Hook, {}", hook.hookName());
    }

    public void registerEndTransactionHook(final EndTransactionHook hook) {
        this.endTransactionHookList.add(hook);
        log.info("register endTransaction Hook, {}", hook.hookName());
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    /**
     * 启动消息生产者
     *
     * @param startFactory 是否开启生产者对应的客户端
     * @throws MQClientException
     */
    public void start(final boolean startFactory) throws MQClientException {
        /**
         * 与标准的状态模式不同的是：它没有使用状态子类，而是使用分支流程(switch-case)来实现不同状态下的不同行为，在管理比较简单的状态时，使用这种设计会让代码更加简洁
         * 说明：
         * 1. 在设计状态的时候，有两个要点是需要注意的
         *  - 1) 不仅要设计正常的状态，还要设计中间状态和异常状态，否则，系统一旦出现异常，你的状态就不准确了，你也就很难处理这种异常状态。
         *       比如这段代码中，RUNNING 和 SHUTDOWN_ALREADY 是正常状态，CREATE_JUST 是一个中间状态，START_FAILED 是一个异常状态。
         *  - 2) 将这些状态之间的转换路径考虑清楚，并在进行状态转换的时候，检查上一个状态是否能够转换到下一个状态。
         *       比如在这里，只有处于CREATE_JUST 状态才能转换为 RUNNING 状态，这样就可以确保这个服务是一次性的，只能启动一次，从而避免了多次启动服务而导致的各种问题。
         */
        switch (this.serviceState) {
            case CREATE_JUST:
                // 将服务状态设置为启动失败，但不是真的启动失败
                this.serviceState = ServiceState.START_FAILED;

                // todo 1. 校验生产者服务启动相关的配置(是对生产者分组名的校验)
                this.checkConfig();

                // todo 生产者服务分组名不是客户端内部生产者分组名时，说明非内部生产者，
                //  将生产者的 defaultMQProducer#instanceName 属性值由 DEFAULT 改为 pid
                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }

                // todo 2.同个一个单例模式的 MQClientManager 创建或获取客户端实例 MQClientInstance
                //  构建参数：MQ生产者实例和rpc钩子 获取或构造MQ客户端实例
                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

                // todo 3.在 MQClientInstance 中注册消息生产者自己，将消息生产者实例添加到 MQClientInstance#producerTable()中
                // 其实消息生产者的职能是交给 客户端实例完成的
                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);

                // 更新消息生产者的状态
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

                // todo 4.存储 Topic 的发布信息，此时 Topic 为 TBW102 。todo 这个是生产者启动就会缓存一份
                // todo 只缓存了 TBW102(这里只有 key (topic)),实际也无详细信息，还是要从 NameSrv 获取
                this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

                // todo 5.启动消息客户端实例 MQClientFactory，里面包含很多服务
                if (startFactory) {
                    // 启动 MQClient
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                    this.defaultMQProducer.isSendMessageWithVIPChannel());

                // 启动成功，todo 服务状态设置为启动成功
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }

        // todo 6.客户端同步向所有的 Broker 发送心跳
        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        /**
         * 定时扫描超时的发送请求，并结束它
         * todo MQClientInstance
         */
        this.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    RequestFutureTable.scanExpiredRequest();
                } catch (Throwable e) {
                    log.error("scan RequestFutureTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());
        // 生产者分组名为 null 抛出异常
        if (null == this.defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }
        // 生产者分组名不能与默认的生产者分组名相同（注意：比较没有区分英文大小写）
        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }
                this.timer.cancel();
                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    /**
     * 获取消息生产者中缓存的 Topic 发布信息，将 Topic 返回
     *
     * @return
     */
    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        for (String key : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * This method will be removed in the version 5.0.0 and <code>getCheckListener</code> is recommended.
     *
     * @return
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    /**
     * 获取事务监听器
     *
     * @return
     */
    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    /**
     * 本地事务状态回查
     *
     * @param addr   Broker 地址
     * @param msg    消息
     * @param header 请求
     */
    @Override
    public void checkTransactionState(final String addr,
                                      final MessageExt msg,
                                      final CheckTransactionStateRequestHeader header) {

        // 回查本地事务状态任务，交给回查线程池处理
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;
            private final MessageExt message = msg;
            private final CheckTransactionStateRequestHeader checkRequestHeader = header;

            // 生产者组
            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

            @Override
            public void run() {
                // 事务检查监听器，用于查询本地事务状态(已废弃)
                TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();

                // todo 获取生产端发送消息时设置的事务监听器
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null) {

                    // 本地事务状态
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        if (transactionCheckListener != null) {
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);

                            // 查询本地事务状态
                        } else if (transactionListener != null) {
                            log.debug("Used new check API in transaction message");
                            // todo 通过事务监听器回查本地事务状态
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        } else {
                            log.warn("CheckTransactionState, pick transactionListener by group[{}] failed", group);
                        }
                    } catch (Throwable e) {
                        log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }

                    // todo 根据本地事务状态，向 Broker 提交消息 Commit / 回滚消息  Rollback
                    this.processTransactionState(
                        localTransactionState,
                        group,
                        exception);
                } else {
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            }

            /**
             * 根据本地事务状态，向 Broker 提交消息 Commit / 回滚消息 Rollback
             *
             * @param localTransactionState 本地事务状态
             * @param producerGroup 生产者组
             * @param exception 检查[本地事务]状态发生的异常
             */
            private void processTransactionState(
                final LocalTransactionState localTransactionState,
                final String producerGroup,
                final Throwable exception) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);

                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (uniqueKey == null) {
                    uniqueKey = message.getMsgId();
                }
                thisHeader.setMsgId(uniqueKey);
                thisHeader.setTransactionId(checkRequestHeader.getTransactionId());

                // 根据本地事务状态确定事务的最终状态
                switch (localTransactionState) {
                    case COMMIT_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                        break;
                    case ROLLBACK_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case UNKNOW:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }

                String remark = null;
                if (exception != null) {
                    remark = "checkLocalTransactionState Exception: " + RemotingHelper.exceptionSimpleDesc(exception);
                }

                // Hook 钩子
                doExecuteEndTransactionHook(msg, uniqueKey, brokerAddr, localTransactionState, true);

                // 向 Broker 端发送 Commit 或 Rollback
                try {
                    DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark,
                        3000);
                } catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }
        };

        // 执行回查 本地事务状态
        this.checkExecutor.submit(request);
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        Validators.checkTopic(newTopic);
        Validators.isSystemTopic(newTopic);

        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public MessageExt viewMessage(
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();

        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * 异步发送消息
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *<P>
     * todo 异步发送已经废弃，原因如下
     * 1. 超时时间计算的不准确
     * 2. 异步方法还有改进的空间，其实可以直接结合 Netty 做到，不需要 Executor
     *
     * @param msg
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws RejectedExecutionException
     */
    @Deprecated
    public void send(final Message msg, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        // 记录开始时间
        final long beginStartTime = System.currentTimeMillis();

        // 获取异步发送消息执行的线程池
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {

            // 当前线程将发送消息任务提交到 executor 中，就可以返回了
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            // 异步发送消息
                            sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(
                            new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * 根据 Broker 选择队列
     *
     * @param tpInfo
     * @param lastBrokerName
     * @return
     */
    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException(
                "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    /**
     * 消息发送
     * <p>
     * todo 消息发送过程中可能出现的问题
     * <p>
     * 1. NameSrv 宕机
     *  - 在发送消息阶段，如果生产者本地缓存中没有缓存 Topic 的路由信息，则需要从 NameSrv 获取，只有当所有的 NameSrv 都不可用时，才会抛 MQClientException。
     *    否则，会重试其他的 NameSrv，客户端(消息生产者、消息消费者)在任意时刻只会和其中一台 NameSrv 建立连接。
     *  - 如果 NameSrv 全部挂掉，并且生产者有缓存 Topic 的路由信息，此时依然可以发送消息。
     *  所以 NameSrv 的宕机，通常不会对整个消息发送带来什么严重的问题。
     * 2. Broker 宕机
     *  - 消息生产者每隔 30s 从 NameSrv 处获取最新的 Broker 存活信息(Topic 路由信息)，Broker 每 30s 向所有的 NameServer 报告自己的情况，故 Broker 的宕机，消息生产者最大的可感知时间是 60s；
     *  - 在这 60s,消息发送会有什么影响呢？
     *    1) 开启 sendLatencyFaultEnable = true ，会对获取的 MQ 进行可用性的验证，比如获取一个 MessageQueue 发送失败，
     *       这时会对该 Broker 进行标记，标记该 Broker 在未来的某段时间内不会被选择到，默认为 5 分钟，不可改变，所以此时只有当该 Topic 全部的 Broker 宕机，才无法发送消息，符合高可用设计。
     *    2) 关闭 sendLatencyFaultEnable = false ，此时会出现消息发送失败的情况，因为默认情况下，producer 每次发送消息，会采取轮询机制取下一个 MessageQueue，
     *       由于可能该 MessageQueue 所在的就是刚才挂掉的 Broker，所以会继续选择下一个 MessageQueue。
     *
     * @param msg 消息
     * @param communicationMode 发送方式：SYNC、ASYNC、ONEWAY
     * @param sendCallback 异步消息发送回调函数
     * @param timeout 消息发送超时时间
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    private SendResult sendDefaultImpl(
        Message msg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // todo 1.确认 Producer (生产者)服务状态，只有运行状态，才可以执行后续的发送消息流程
        this.makeSureStateOK();

        // todo 2.再次校验消息内容：检查消息是否合法，以及消息格式和大小
        Validators.checkMessage(msg, this.defaultMQProducer);

        // 调用编号
        final long invokeID = random.nextLong();

        // 发送消息的开始时间
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        long endTimestamp = beginTimestampFirst;

        // todo 3.查找消息中 Topic 对应的 Topic 发布信息
        //  是对 Topic 路由信息的解析，请注意这里是-写队列
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false;

            // 最后选择消息要发送到的队列
            MessageQueue mq = null;
            Exception exception = null;

            // 最后一次发送结果
            SendResult sendResult = null;

            // 同步发送，timesTotal 字段值为 3，表示会重试两次，消息发送
            // 异步发送，timesTotal 字段值为 1
            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            // 第几次发送
            int times = 0;

            // 存储每次发送消息选择的 Broker 的名称
            String[] brokersSent = new String[timesTotal];

            // 循环重试发送消息，直到成功或超时
            for (; times < timesTotal; times++) {

                // lastBrokerName 就是上一次选择的执行发送消息失败的 Broker
                String lastBrokerName = null == mq ? null : mq.getBrokerName();

                // todo 4. 根据负载均衡算法，选择一个 MessageQueue，即选择消息要发送到的队列
                // todo 这里没有指定队列选择器，使用的是默认的选择队列逻辑，结合是否开启延迟规避策略，使用随机递增取模选择队列
                // todo 其实 RocketMQ 提供了很多 MessageQueueSelector 的实现，例如随机选择策略，参数哈希取模选择策略和同机房选择策略，如果需要，可以自己实现选择策略
                // todo 注意和消费端 MessageQueue 负载算法的区别，两者一个是消息生产端、一个是消息消费端
                // 该消息队列来源于 topicPublishInfo#messageQueueList
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                if (mqSelected != null) {
                    mq = mqSelected;
                    // 存储此次消息发送选择的 Broker
                    brokersSent[times] = mq.getBrokerName();
                    try {
                        // 消息发送开始时间
                        beginTimestampPrev = System.currentTimeMillis();
                        if (times > 0) {
                            //Reset topic with namespace during resend.
                            msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
                        }

                        // 消息发送耗时
                        long costTime = beginTimestampPrev - beginTimestampFirst;

                        // todo 消息发送超时，消息就不会再进行发送重试
                        if (timeout < costTime) {
                            callTimeout = true;
                            break;
                        }

                        // todo 调用 sendKernelImpl 方法，向选中的写消息队列，发送消息
                        // 通过 Producer 与 Broker 的长连接将消息发送给 Broker，然后Broker 将消息存储，并对发送结果进行封装返回
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                        endTimestamp = System.currentTimeMillis();

                        /*------ 说明：endTimestamp - beginTimestampPrev ，表示消息发送延迟时间*/

                        // todo 消息发送成功，根据发送时长，更新 Broker 的可用信息
                        // todo 发送消息后，更新 Broker 的可用信息，用来干嘛？在选择发送消息时的消息队列时，会参考 Broker 的可用信息
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        switch (communicationMode) {
                            // 消息异步发送的重试是在 回调方法里面，并且只会在当前 Broker 重试
                            case ASYNC:
                                return null;
                            case ONEWAY:
                                return null;
                            // 消息同步发送，支持重试机制，通过 retryTimesWhenSendFailed 参数设置，默认为 2 ，表示重试 2 次，实际上也就是最多发送 3 次
                            case SYNC:
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    // todo 消息发送失败 && 开启重试其他Broker开关，此时会选择其他 Broker 进行重试发送
                                    //  否则，只会在 客户端异常、网络异常的情况下，才会进行发送重试
                                    if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                        continue;
                                    }
                                }

                                return sendResult;
                            default:
                                break;
                        }

                        // todo 如果在发送过程中抛出了异常，则更新 Broker 可用信息，用来规避发生故障的 Broker
                        //  并根据不同的异常信息，选择是否进行重试，一般是消息发送方的问题才会重试，如：网络异常(RemotingException)、客户端异常(MQClientException)
                    } catch (RemotingException e) {
                        endTimestamp = System.currentTimeMillis();
                        // 发送失败，消息队列对应的Broker
                        // 本次消息发送延迟时间
                        // 是否隔离 Broker ，如果该参数为 true，则使用默认时长 30s 来计算 Broker 不可用时长；如果该参数为 false，则使用本次消息发送延迟时间，来计算Broker 的不可用时长
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQClientException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                        /**
                         * broker异常，直接抛出，不再进行重试
                         */
                    } catch (MQBrokerException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        switch (e.getResponseCode()) {
                            case ResponseCode.TOPIC_NOT_EXIST:
                            case ResponseCode.SERVICE_NOT_AVAILABLE:
                            case ResponseCode.SYSTEM_ERROR:
                            case ResponseCode.NO_PERMISSION:
                            case ResponseCode.NO_BUYER_ID:
                            case ResponseCode.NOT_IN_CURRENT_UNIT:
                                continue;
                            default:
                                if (sendResult != null) {
                                    return sendResult;
                                }

                                throw e;
                        }
                        /**
                         * 中断异常，直接抛出，不再进行重试
                         */
                    } catch (InterruptedException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        log.warn(String.format("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());

                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        throw e;
                    }
                } else {
                    break;
                }
            }

            // 发送消息是否成功
            if (sendResult != null) {
                return sendResult;
            }

            /*------------------ todo 执行到下面这些处理逻辑，说明消息发送失败了，下面的处理逻辑是为了判断什么原因导致的失败------------------*/

            // 打印消息发送失败的日志，内容包含发送几次消息、耗时多少、消息所属的 Topic，发送消息时选择的 Broker
            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                times,
                System.currentTimeMillis() - beginTimestampFirst,
                msg.getTopic(),
                Arrays.toString(brokersSent));

            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

            // todo 根据不同情况抛出不同的异常
            MQClientException mqClientException = new MQClientException(info, exception);
            // todo 判断消息是否发送超时
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }

            if (exception instanceof MQBrokerException) {
                mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
            } else if (exception instanceof RemotingConnectException) {
                mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
            } else if (exception instanceof RemotingTimeoutException) {
                mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
            } else if (exception instanceof MQClientException) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }

            throw mqClientException;
        }

        // 执行到这里说明，Topic 的路由信息都没有找到

        // 校验 nameSrv 地址列表
        validateNameServerSetting();

        // Topic 的路由信息找不到异常
        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
            null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }

    /**
     * 获取 Topic 的发布信息，是对 Topic 路由信息的解析和封装
     * <p>
     * todo 特别说明：在 RocketMQ 开启自动创建主题的机制下(默认开启)
     * 1. 消息发送者向一个不存在的主题发送消息时，向 NameSrv 查询该主题的路由信息，会返回空
     * 2. 然后使用默认的主题名 TBW102 再次从 NameSrv 查询路由信息
     * 3. 然后发送者会使用默认主题的路由信息进行负载均衡，就是说该路由信息中的消息队列是属于默认主题的，而不是我们要发送消息的主题
     * 4. 注意：是使用默认主题的路由信息，而不是直接使用默认路由信息作为新主题创建对应的路由信息
     * @param topic
     * @return
     */
    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        // todo 从生产者本地缓存中尝试获取 Topic 路由信息
        //  注意：Producer启动后，会添加默认的Topic ：TBW102，但是具体的信息没有，还是需要从 NameSrv 中获取
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);

        // 本地缓存没有命中，尝试从 NameSrv 中获取一次
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            // 创建消息使用的 Topic 对应的 TopicPublishInfo 对象，并放入 topicPublishInfoTable
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());

            // 从 NameSrv 中获取该 Topic 信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);

            // 再次获取 Topic 的路由信息
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }

        // 如果获取到的 topicPublishInfo 可用，直接返回
        // topicPublishInfo 是否可用的判断标准：topicPublishInfo 存在 topic 路由信息，并且存在消息队列
        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;

            // 如果未找到路由信息，则再次尝试使用默认的 Topic(TBW102) 去找路由配置信息，然后将默认的路由信息作为 Topic 的路由信息。
            // 即向 NameSrv 获取 Topic 为 TBW102 的路由配置信息，作为 TopicTest 的路由信息
            /** 使用 {@link DefaultMQProducer#createTopicKey} 对应的 Topic 发布信息。用于 Topic 发布信息不存在 && Broker 支持自动创建 Topic*/
        } else {
            // todo 如果根据 topic 获取到的 topicPublishInfo 不存在 topic 路由信息或者不存在消息队列，从 NameServer 中更该 topic 的路由信息
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    /**
     * 发送消息的核心方法 - 该方法会真正的使用网络发起请求
     * 说明：该方法会真正发起网络请求，将消息发送给 Broker
     * <p>
     * todo 发送消息选择的都是主节点
     *
     * @param msg               待发送消息
     * @param mq                消息将被发送到该消息队列上
     * @param communicationMode 消息发送模式 Sync/Async/Oneway
     * @param sendCallback      异步消息回调函数
     * @param topicPublishInfo  Topic 发布信息
     * @param timeout           消息发送超时时间
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    private SendResult sendKernelImpl(
                                    final Message msg,
                                    final MessageQueue mq,
                                    final CommunicationMode communicationMode,
                                    final SendCallback sendCallback,
                                    final TopicPublishInfo topicPublishInfo,
                                    final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();


        // 根据 Broker 名称在 brokerAddrTable 中获取 Broker 地址，注意：选择的是 Broker 主节点地址
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            // 根据 topic 重新更新 TopicPublishInfo，
            // 该方法里面，会根据 TopicRouteData 里面的信息 重新覆盖 brokerAddrTable 中对应的 Broker 名称和地址
            tryToFindTopicPublishInfo(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        // 发送消息上下文
        SendMessageContext context = null;
        if (brokerAddr != null) {
            // 是否使用 Broker VIP 通道。Broker 会开启两个端口对外服务
            brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

            // 记录消息内容，下面的逻辑，可能会改变消息内容，例如消息压缩
            byte[] prevBody = msg.getBody();
            try {
                //for MessageBatch,ID has been set in the generating process
                // 对于批量消息，在生成过程中已设置 ID
                // 对于非批量消息，为消息分配全局唯一Id
                if (!(msg instanceof MessageBatch)) {
                    // todo msg 的 UNIQ_KEY 属性值，该Id 是消息发送者在消息发送时，会首先在客户端生成，全局唯一，即 msgId
                    MessageClientIDSetter.setUniqID(msg);
                }

                boolean topicWithNamespace = false;
                if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                    msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
                    topicWithNamespace = true;
                }

                // 消息类型
                int sysFlag = 0;
                // 消息压缩标识
                boolean msgBodyCompressed = false;

                // 默认对 >= 4k 的消息进行压缩
                if (this.tryToCompressMessage(msg)) {
                    // 设置消息的系统标记为 MessageSysFlag.COMPRESSED_FLAG
                    sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                    msgBodyCompressed = true;
                }

                // todo 如果是事务 Prepared 消息（根据属性中的 TRAN_MSG 属性判断）
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (tranMsg != null && Boolean.parseBoolean(tranMsg)) {
                    // 设置消息的系统标记为 MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                }

                // hook：发送消息校验
                if (hasCheckForbiddenHook()) {
                    CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                    checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
                    checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
                    checkForbiddenContext.setCommunicationMode(communicationMode);
                    checkForbiddenContext.setBrokerAddr(brokerAddr);
                    checkForbiddenContext.setMessage(msg);
                    checkForbiddenContext.setMq(mq);
                    checkForbiddenContext.setUnitMode(this.isUnitMode());
                    this.executeCheckForbiddenHook(checkForbiddenContext);
                }

                // 如果注册了消息发送钩子函数，则执行消息发送之前的增强逻辑
                // hook：消息发送前的逻辑
                if (this.hasSendMessageHook()) {
                    context = new SendMessageContext();
                    context.setProducer(this);
                    context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    context.setCommunicationMode(communicationMode);
                    context.setBornHost(this.defaultMQProducer.getClientIP());
                    context.setBrokerAddr(brokerAddr);
                    context.setMessage(msg);
                    context.setMq(mq);
                    context.setNamespace(this.defaultMQProducer.getNamespace());
                    String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (isTrans != null && isTrans.equals("true")) {
                        context.setMsgType(MessageType.Trans_Msg_Half);
                    }

                    if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
                        context.setMsgType(MessageType.Delay_Msg);
                    }

                    // 执行发送消息之前的钩子方法实现
                    this.executeSendMessageHookBefore(context);
                }

                // todo 构建消息发送请求头 SendMessageRequestHeader
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                // 生产者组名
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                // 消息主题
                requestHeader.setTopic(msg.getTopic());
                // todo 默认主题：TBW102
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                // todo 主题在单个 Broker 上的默认队列数，默认为 4
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                // 消息队列Id(消息队列序号)
                requestHeader.setQueueId(mq.getQueueId());
                // 消息系统标记
                requestHeader.setSysFlag(sysFlag);
                // 消息发送时间戳
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                // 消息标记(RocketMQ 对消息中的标记不做任何处理，供应用程序使用)
                requestHeader.setFlag(msg.getFlag());

                // 消息扩展属性
                // todo 包含很多重要的附加功能，如 topic 、queueId 替换套路、tag 存储
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));

                // todo 消息的重试次数，如果是第一次发送，则是 0
                requestHeader.setReconsumeTimes(0);
                requestHeader.setUnitMode(this.isUnitMode());

                // 是否是批量消息
                requestHeader.setBatch(msg instanceof MessageBatch);

                // todo 如果是重试的 Topic，设置重试次数，其中在消费失败时，消费者可能会使用内部维护的生产者发送消息，这里就是处理这个的
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    // 设置重试次数
                    String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                    if (reconsumeTimes != null) {
                        // 设置重试次数
                        requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                        // 清除消息中的重试次数
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                    }

                    // 最大重试次数
                    String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                    if (maxReconsumeTimes != null) {
                        // 设置最大重试次数
                        requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                        // 清除消息中的最大重试次数
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                    }
                }

                // todo 根据消息发送模式(Sync、ASYNC、OneWay)，进行网络传输
                SendResult sendResult = null;
                switch (communicationMode) {
                    // 异步
                    case ASYNC:
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            msg.setBody(prevBody);
                        }

                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                        }

                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        // todo 将消息发送给 Topic 所属写消息队列所在的 Broker
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            mq.getBrokerName(),
                            tmpMessage,
                            requestHeader,
                            timeout - costTimeAsync,
                            communicationMode,
                            sendCallback,
                            topicPublishInfo,
                            this.mQClientFactory,
                            this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
                            context,
                            this);
                        break;
                    case ONEWAY:
                    case SYNC:
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        // 同步发送
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            mq.getBrokerName(),
                            msg,
                            requestHeader,
                            timeout - costTimeSync,
                            communicationMode,
                            context,
                            this);
                        break;
                    default:
                        assert false;
                        break;
                }

                // 如果注册了消息发送钩子函数
                // hook ，发送消息后的逻辑
                if (this.hasSendMessageHook()) {
                    context.setSendResult(sendResult);
                    this.executeSendMessageHookAfter(context);
                }

                // 返回发送结果
                return sendResult;
            } catch (RemotingException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (MQBrokerException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (InterruptedException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } finally {
                // 防止多次压缩消息体
                msg.setBody(prevBody);
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
            }
        }

        // 抛出 Broker 不存在异常
        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    /**
     * 尝试压缩单条消息
     *
     * @param msg
     * @return
     */
    private boolean tryToCompressMessage(final Message msg) {
        // 批量消息不压缩
        if (msg instanceof MessageBatch) {
            //batch dose not support compressing right now
            return false;
        }

        byte[] body = msg.getBody();
        if (body != null) {
            // 默认对消息体超过 4kb 采用 zip 压缩
            if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    byte[] data = UtilAll.compress(body, zipCompressLevel);
                    if (data != null) {
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }

    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
        if (hasCheckForbiddenHook()) {
            for (CheckForbiddenHook hook : checkForbiddenHookList) {
                hook.checkForbidden(context);
            }
        }
    }

    public boolean hasSendMessageHook() {
        return !this.sendMessageHookList.isEmpty();
    }

    public void executeSendMessageHookBefore(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookBefore", e);
                }
            }
        }
    }

    public void executeSendMessageHookAfter(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookAfter", e);
                }
            }
        }
    }

    public boolean hasEndTransactionHook() {
        return !this.endTransactionHookList.isEmpty();
    }

    public void executeEndTransactionHook(final EndTransactionContext context) {
        if (!this.endTransactionHookList.isEmpty()) {
            for (EndTransactionHook hook : this.endTransactionHookList) {
                try {
                    hook.endTransaction(context);
                } catch (Throwable e) {
                    log.warn("failed to executeEndTransactionHook", e);
                }
            }
        }
    }

    public void doExecuteEndTransactionHook(Message msg, String msgId, String brokerAddr, LocalTransactionState state,
        boolean fromTransactionCheck) {
        if (hasEndTransactionHook()) {
            EndTransactionContext context = new EndTransactionContext();
            context.setProducerGroup(defaultMQProducer.getProducerGroup());
            context.setBrokerAddr(brokerAddr);
            context.setMessage(msg);
            context.setMsgId(msgId);
            context.setTransactionId(msg.getTransactionId());
            context.setTransactionState(state);
            context.setFromTransactionCheck(fromTransactionCheck);
            executeEndTransactionHook(context);
        }
    }
    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
            throw new MQClientException("message's topic not equal mq's topic", null);
        }

        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout < costTime) {
            throw new RemotingTooMuchRequestException("call timeout");
        }

        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
    }

    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        makeSureStateOK();
                        Validators.checkMessage(msg, defaultMQProducer);

                        if (!msg.getTopic().equals(mq.getTopic())) {
                            throw new MQClientException("message's topic not equal mq's topic", null);
                        }
                        long costTime = System.currentTimeMillis() - beginStartTime;
                        if (timeout > costTime) {
                            try {
                                sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null,
                                    timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknown exception", e);
                            }
                        } else {
                            sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                        }
                    } catch (Exception e) {
                        sendCallback.onException(e);
                    }

                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    /**
     * 向选择的消息队列中发送消息
     * todo 使用自定义的消息发送负载算法，RocketMQ 消息发送内部的重试机制将失效，请再调用该方法的上层进行重试
     *
     * @param msg               消息
     * @param selector          队列选择器
     * @param arg               参数
     * @param communicationMode 消息发送模式
     * @param sendCallback      回调函数
     * @param timeout           消息发送超时时间
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    private SendResult sendSelectImpl(
        Message msg,
        MessageQueueSelector selector,
        Object arg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback, final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 开始时间
        long beginStartTime = System.currentTimeMillis();
        // Producer 状态检查
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        // 根据 Topic 找到对应的 Topic 发布信息
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());

        // 如果 Topic 的发布信息不为空
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                // 获取 Topic 的写消息队列
                List<MessageQueue> messageQueueList = mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());

                // 克隆消息
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
                userMessage.setTopic(userTopic);

                // todo 通过队列选择器从消息队列中选择一个写消息队列
                mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
            } catch (Throwable e) {
                throw new MQClientException("select message queue threw exception.", e);
            }

            // 选择写消息队列之后的耗时
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }

            // todo 向选中的消息队列发送消息，即 producer 顺序发送消息
            if (mq != null) {
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        // 验证 NameSrv 配置是否存在
        validateNameServerSetting();
        // 抛出 Topic 路由信息不存在的异常
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            try {
                                sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback,
                                    timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknownn exception", e);
                            }
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("exector rejected ", e);
        }
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * 发送事务消息
     * todo 特别说明：RocketMQ 实现发送事务消息：
     * 1. 第一阶段发送 Prepared 消息
     * 2. 第二阶段执行本地事务
     * 3. 第三阶段通过第一阶段拿到的地址访问消息，并修改消息的状态
     * <p>
     * 工作：
     * 1. 给消息打上事务消息标签，即设置属性 TRAM_MSG，用于 Broker 区分普通消息和事务消息
     * 2. 发送半消息(half message)
     * 3. 发送成功，则由 TransactionListener 执行本地事务
     * 4. 执行 endTransaction 方法，告诉 Broker 执行 commit / rollback
     *
     * @param msg                      消息
     * @param localTransactionExecuter 本地事务执行器
     * @param arg                      本地事务执行器参数 即 listener 参数
     * @return 事务消息发送结果
     * @throws MQClientException
     */
    public TransactionSendResult sendMessageInTransaction(final Message msg,
                                                          final LocalTransactionExecuter localTransactionExecuter,
                                                          final Object arg) throws MQClientException {
        // 判断检查本地事务的 listener 是否存在
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionExecuter && null == transactionListener) {
            throw new MQClientException("tranExecutor is null", null);
        }

        // todo 事务消息不支持延迟消息，如果是事务消息设置了延迟级别，延迟级别将会被忽略
        // ignore DelayTimeLevel parameter
        if (msg.getDelayTimeLevel() != 0) {
            // 移除 msg 中的 DELAY 参数，不支持延迟消息
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }
        Validators.checkMessage(msg, this.defaultMQProducer);

        SendResult sendResult = null;
        // todo 重要：msg 设置参数 TRAN_MSG 表示为事务消息，也就是半消息
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        // todo 设置消息处于哪个生产者组 PGROUP
        // todo 设置生产者组是为了事务消息回查本地事务状态，从该生产者组中随机选择一个生产者进行事务回查
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        try {
            // todo 事务消息第一阶段：调用发送普通消息的方法，同步发送 Prepare 消息到 Half 主题下的 0 号队列(只有一个队列)
            sendResult = this.send(msg);
        } catch (Exception e) {
            // 发送半消息异常，直接抛出异常
            throw new MQClientException("send message Exception", e);
        }

        // 处理发送 Half 消息的结果，默认是中间状态
        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        switch (sendResult.getSendStatus()) {
            // 发送 Half 消息成功，才会执行本地事务，也就是执行业务代码实现，否则不会执行本地事务
            case SEND_OK: {
                try {
                    // 事务编号
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }

                    // UNIQ_KEY ，这是客户端发送的时候生成的一个唯一 Id，也就是我们平常用的 sendResult 里面的 msgId
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }

                    // todo 事务消息第二阶段：如果发送 Prepare 消息成功，处理与消息关联的本地事务
                    //  特别注意：1. 使用本地事务执行器执行本地事务逻辑，这样的方式已经被废弃了
                    //       2.新版本都是使用事务监听器(TransactionListener)执行本地事务
                    if (null != localTransactionExecuter) {
                        localTransactionState = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);

                        // 使用事务监听器执行本地事务
                    } else if (transactionListener != null) {
                        log.debug("Used new transaction API");
                        localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                    }

                    // 返回 null，则认为是未知
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    // 如果不是事务提交状态，则打印日志
                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return {}", localTransactionState);
                        log.info(msg.toString());
                    }

                    // 本地事务处理异常
                } catch (Throwable e) {
                    log.info("executeLocalTransactionBranch exception", e);
                    log.info(msg.toString());
                    localException = e;
                }
            }
            break;

            // 发送 Half 消息失败，则标记本地事务状态为回滚
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }

        // todo 事务消息第三阶段：发送确认消息，即将请求发往 Broker 去更新事务消息的最终状态，使用 op 消息打标的方式
        // 如果 endTransaction 方法执行失败，数据没有发送到 Broker，导致事务消息的状态更新失败，Broker 会有回查线程定时(默认 1分钟) 扫描每个存储事务消息的文件
        // 已经提交或回滚的消息直接跳过，如果是 Prepare状态，则会向 producer 发送 checkTransaction 请求，Producer 会调用 DefaultMQProducerImpl.checkTransactionState() 方法来处理 Broker 的定时回调请求
        // 而 checkTransactionState 方法会调用我们的事务设置的决断方法来决定是回滚事务，还是继续执行，最后调用 endTransactionOneway 让 Broker 来更新事务消息的最终状态(op 消息打标)
        // 结束事务，根据事务消息发送的结果和本地事务执行情况，决定是 Commit 或 Rollback
        try {
            // 执行 endTransaction 方法
            // - 半消息发送失败 || 本地事务执行失败，则告诉服务端(Broker)是删除半消息(op消息打标)
            // - 半消息发送成功 && 本地事务执行成功，则告诉服务端(Broker)是生效半消息(OP消息达标)
            // - 未知状态，等待 Broker 回查即可
            this.endTransaction(msg, sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        // 返回事务消息发送结果
        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        
        // 消息Id
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        // 事务Id
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        // 本地事务执行状态
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * 结束事务
     * 根据事务消息的发送结果 和 本地事务执行结果，决定是 Commit 还是 Rollback
     * 即：
     * - 半消息发送失败 || 本地事务执行失败，则告诉服务端(Broker)是删除半消息(op消息打标)
     * - 半消息发送成功 && 本地事务执行成功，则告诉服务端(Broker)是生效半消息(OP消息达标)
     * - 半消息发送成功 && 本地事务状态未知，则等待 Broker 回查即可
     *
     * @param msg                   Half 消息
     * @param sendResult            Half 消息发送的结果
     * @param localTransactionState 本地事务执行的结果
     * @param localException        执行本地事务逻辑产生的异常
     * @throws RemotingException    当远程调用发生异常时
     * @throws MQBrokerException    当 Broker 发送异常时
     * @throws InterruptedException 当线程中断时
     * @throws UnknownHostException 当解码消息编号失败时
     */
    public void endTransaction(
        final Message msg,
        final SendResult sendResult,
        final LocalTransactionState localTransactionState,
        final Throwable localException) throws RemotingException, MQBrokerException, InterruptedException, UnknownHostException {
        final MessageId id;
        
        // 解码消息编号，这个是服务端的 offsetMsgId，包含当前消息的绝对信息：所属的Broker + 消息在 CommitLog 中的偏移量
        if (sendResult.getOffsetMsgId() != null) {
            id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }

        // 获取事务Id
        String transactionId = sendResult.getTransactionId();

        // 根据 Broker 名称获取 BrokerAddr
        // todo 半消息发送到了哪个 Broker 上，最后提交也得到这个 Broker 上（发送消息都是使用的是 Broker 的主节点）
        final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());

        // 创建请求
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        // 事务Id
        requestHeader.setTransactionId(transactionId);
        // 消息在 CommitLog 中的物理偏移量
        requestHeader.setCommitLogOffset(id.getOffset());

        /**
         * todo 两段式协议发送与提交/回滚消息
         *  执行完本地事务消息的状态为 unkonw 时，结束事务不做任何操作
         *  即 Broker 端的 EndTransactionProcessor 不对 UNKNOW 映射的 TRANSACTION_NOT_TYPE 进行处理，
         *  而是通过 TransactionalMessageCheckService 线程定时去检测 RMQ_SYS_TRANS_HALF_TOPIC 主题中的消息，回查消息的事务状态
         */
        switch (localTransactionState) {
            // 提交
            case COMMIT_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            // 回滚
            case ROLLBACK_MESSAGE:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            // 未知
            case UNKNOW:
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }

        // Hook
        doExecuteEndTransactionHook(msg, sendResult.getMsgId(), brokerAddr, localTransactionState, false);
        // 生产者组
        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        // 消息在 MessageQueue 中的逻辑偏移量
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        // 消息Id
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;

        /**
         * todo 结束事务，注意：这里是发送一个单向的 rpc 请求，告知 Broker 完成事务的提交或回滚。
         *  由于事务消息有反查机制的兜底，这个 rpc 请求即使失败或者丢失，也不会影响到事务消息的最终状态
         */
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark, this.defaultMQProducer.getSendMsgTimeout());
    }

    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
    }

    public ExecutorService getAsyncSenderExecutor() {
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    /**
     * 同步发送消息
     *
     * @param msg     消息
     * @param timeout 超时时间
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws MQBrokerException
     * @throws InterruptedException
     */
    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 同步发送消息
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

    public Message request(Message msg,
        long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(Message msg, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);
    }

    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback, final long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);

    }

    public Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, null, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureTable.getRequestFutureTable().remove(correlationId);
        }
    }

    private Message waitResponse(Message msg, long timeout, RequestResponseFuture requestResponseFuture, long cost) throws InterruptedException, RequestTimeoutException, MQClientException {
        Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
        if (responseMessage == null) {
            if (requestResponseFuture.isSendRequestOk()) {
                throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                    "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
            } else {
                throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
            }
        }
        return responseMessage;
    }

    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureTable.getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, null, timeout - cost);
    }

    private void requestFail(final String correlationId) {
        RequestResponseFuture responseFuture = RequestFutureTable.getRequestFutureTable().remove(correlationId);
        if (responseFuture != null) {
            responseFuture.setSendRequestOk(false);
            responseFuture.putResponseMessage(null);
            try {
                responseFuture.executeRequestCallback();
            } catch (Exception e) {
                log.warn("execute requestCallback in requestFail, and callback throw", e);
            }
        }
    }

    private void prepareSendRequest(final Message msg, long timeout) {
        String correlationId = CorrelationIdUtil.createCorrelationId();
        String requestClientId = this.getmQClientFactory().getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        boolean hasRouteData = this.getmQClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }

    public int getZipCompressLevel() {
        return zipCompressLevel;
    }

    public void setZipCompressLevel(int zipCompressLevel) {
        this.zipCompressLevel = zipCompressLevel;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }
}
