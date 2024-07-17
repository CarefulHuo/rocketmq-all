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
package org.apache.rocketmq.client.impl.factory;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.admin.MQAdminExtInner;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.ClientRemotingProcessor;
import org.apache.rocketmq.client.impl.FindBrokerResult;
import org.apache.rocketmq.client.impl.MQAdminImpl;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.MQConsumerInner;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullMessageService;
import org.apache.rocketmq.client.impl.consumer.RebalanceService;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.stat.ConsumerStatsManager;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumerData;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.ProducerData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 1. RocketMQ 客户端中的顶层类，大多数情况下，可以简单理解为每个客户端对应一个 MQClientInstance 实例
 * 2. 消息客户端实例，封装对 NameSrv、Broker 的 API 调用，提供给 Producer、Consumer 使用
 */
public class MQClientInstance {
    private final static long LOCK_TIMEOUT_MILLIS = 3000;
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 配置信息
     */
    private final ClientConfig clientConfig;

    /**
     * MQClientInstance 在同一台机器上的创建序号
     */
    private final int instanceIndex;

    /**
     * 客户端Id：即可能是生产者Id，也可能是消费者Id
     */
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();

    /**
     * 生产者组 到 消息生产者的映射
     */
    private final ConcurrentMap<String/* group */, MQProducerInner> producerTable = new ConcurrentHashMap<String, MQProducerInner>();

    /**
     * 消费者组 到 消息消费者的映射
     */
    private final ConcurrentMap<String/* group */, MQConsumerInner> consumerTable = new ConcurrentHashMap<String, MQConsumerInner>();

    /**
     * 主要处理运维命令
     */
    private final ConcurrentMap<String/* group */, MQAdminExtInner> adminExtTable = new ConcurrentHashMap<String, MQAdminExtInner>();

    /**
     * 网络通信配置
     */
    private final NettyClientConfig nettyClientConfig;

    /**
     * MQ 网络通信客户端封装类
     */
    private final MQClientAPIImpl mQClientAPIImpl;

    /**
     * MQ 管理命令实现类
     */
    private final MQAdminImpl mQAdminImpl;

    /**
     * Topic 路由信息(原始的，从 NameSrv 获取的)
     * todo 客户端实例缓存的是从 NameSrv 拉取的 Topic 路由信息的元数据；而实例对应的生产者或消费者缓存的是 Topic 路由信息的加工版本--> Topic 发布信息
     * - TopicRouteData 中的 QueueData 没有 Topic 信息
     * - MessageQueue 中有 Topic 信息
     */
    private final ConcurrentMap<String/* Topic */, TopicRouteData> topicRouteTable = new ConcurrentHashMap<String, TopicRouteData>();
    private final Lock lockNamesrv = new ReentrantLock();
    private final Lock lockHeartbeat = new ReentrantLock();

    /**
     * Topic 路由信息中的 Broker 信息，存储在 NameServer ，缓存在本地客户端，供生产者、消费者共同使用
     * 即 Broker 名称和 Broker 地址相关 Map
     * todo 客户端与 Broker 通信，需要通过 MessageQueue 中的 BrokerName 结合当前缓存信息，确定具体的 Broker 地址
     */
    private final ConcurrentMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
        new ConcurrentHashMap<String, HashMap<Long, String>>();
    private final ConcurrentMap<String/* Broker Name */, HashMap<String/* address */, Integer>> brokerVersionTable =
        new ConcurrentHashMap<String, HashMap<String, Integer>>();



    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "MQClientFactoryScheduledThread");
        }
    });

    /**
     * 远程通信客户端请求处理器
     */
    private final ClientRemotingProcessor clientRemotingProcessor;

    /**
     * 拉取消息任务，一个 MQClientInstance, 只会启动一个消息拉取任务
     */
    private final PullMessageService pullMessageService;

    /**
     * 均衡消息队列服务定时任务
     */
    private final RebalanceService rebalanceService;

    /**
     * 消息生产者默认实例
     */
    private final DefaultMQProducer defaultMQProducer;

    /**
     * 消费端统计
     */
    private final ConsumerStatsManager consumerStatsManager;

    /**
     * 心跳包发送次数
     */
    private final AtomicLong sendHeartbeatTimesTotal = new AtomicLong(0);

    /**
     * 客户端状态
     */
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private Random random = new Random();

    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId) {
        this(clientConfig, instanceIndex, clientId, null);
    }

    /**
     * 客户端实例封装了 RocketMQ 的网络处理 API，是生产者、消费者与 NameSrv 、Broker 交互的网络通道
     * @param clientConfig
     * @param instanceIndex
     * @param clientId
     * @param rpcHook
     */
    public MQClientInstance(ClientConfig clientConfig, int instanceIndex, String clientId, RPCHook rpcHook) {
        // MQ客户端配置
        this.clientConfig = clientConfig;
        // MQ客户端实例索引，每个实例对应一个
        this.instanceIndex = instanceIndex;

        /**---------- 封装网络 API start---------*/

        // netty客户端配置
        this.nettyClientConfig = new NettyClientConfig();
        // netty客户端配置-设置客户端回调执行线程数
        this.nettyClientConfig.setClientCallbackExecutorThreads(clientConfig.getClientCallbackExecutorThreads());
        // netty客户端配置-设置TLS是否启用，默认 false
        this.nettyClientConfig.setUseTLS(clientConfig.isUseTLS());
        // 创建客户端远程处理器，将 MQClientFactory 实例注入到 ClientRemotingProcessor 实例中
        this.clientRemotingProcessor = new ClientRemotingProcessor(this);

        /**
         * todo 创建客户端API实例，nettyClientConfig、clientRemotingProcessor、Rpc钩子、客户端配置，注入到 MQClientAPIImpl 实例中
         *  封装网络处理的客户端
         *  客户端实例创建时，创建 网络通信客户端封装类
         */
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig, this.clientRemotingProcessor, rpcHook, clientConfig);

        /**---------- 封装网络 API end---------*/


        // todo 更新 NameSrv 地址
        if (this.clientConfig.getNamesrvAddr() != null) {
            // 更新 NameServerAddressList 集合
            this.mQClientAPIImpl.updateNameServerAddressList(this.clientConfig.getNamesrvAddr());
            log.info("user specified name server address: {}", this.clientConfig.getNamesrvAddr());
        }

        this.clientId = clientId;

        // 创建消息管理实例
        this.mQAdminImpl = new MQAdminImpl(this);

        // todo 创建消息拉取实例
        this.pullMessageService = new PullMessageService(this);

        // todo 创建均衡消息队列任务
        this.rebalanceService = new RebalanceService(this);

        // todo 创建生产者组名为 CLIENT_INNER_PRODUCER 的生产者
        this.defaultMQProducer = new DefaultMQProducer(MixAll.CLIENT_INNER_PRODUCER_GROUP);
        this.defaultMQProducer.resetClientConfig(clientConfig);

        // todo 创建消费者统计管理器
        this.consumerStatsManager = new ConsumerStatsManager(this.scheduledExecutorService);

        log.info("Created a new client Instance, InstanceIndex:{}, ClientID:{}, ClientConfig:{}, ClientVersion:{}, SerializerType:{}",
            this.instanceIndex,
            this.clientId,
            this.clientConfig,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION), RemotingCommand.getSerializeTypeConfigInThisServer());
    }

    /**
     * todo Topic 路由信息转换成 Topic 发布信息，也就是将 Topic 元数据信息和 Topic 分布的 Broker 元数据融合成 Topic 发布信息
     * 1. Topic 的路由：Topic 对应队列信息和 Broker 信息
     * 2. 根据 Topic 的路由，创建写队列集合 -- 写消息队列诞生
     *
     * @param topic topic
     * @param route 路由信息
     * @return
     */
    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {

        // topic 发布信息
        TopicPublishInfo info = new TopicPublishInfo();

        // 保存从 NameSrv 拉取的原始的路由信息
        info.setTopicRouteData(route);

        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMessageQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        } else {

            // todo 从路由信息中获取队列信息，注意：这是 topic 下所有队列信息，可能这些队列分布在不同的 Broker 上
            List<QueueData> qds = route.getQueueDatas();
            Collections.sort(qds);

            // todo 遍历路由信息的队列集合，转为 MessageQueue
            // todo 注意队列数
            for (QueueData qd : qds) {
                // 当前队列支持写，才有意义，因为这些队列是生产者使用的
                if (PermName.isWriteable(qd.getPerm())) {

                    // Broker 信息
                    BrokerData brokerData = null;

                    // 遍历路由信息的 Broker 集合，寻找当前 队列 对应的 BrokerData
                    for (BrokerData bd : route.getBrokerDatas()) {

                        // 根据队列所在的 Broker 名称，找到对应的 Broker 信息
                        if (bd.getBrokerName().equals(qd.getBrokerName())) {
                            brokerData = bd;
                            break;
                        }
                    }

                    // todo 队列没有对应的 Broker，说明当前队列无效
                    if (null == brokerData) {
                        continue;
                    }

                    // todo 队列对应的 Broker 需要是主节点，否则不会组装对应的 topic 信息
                    if (!brokerData.getBrokerAddrs().containsKey(MixAll.MASTER_ID)) {
                        continue;
                    }

                    // todo 根据当前队列的写队列数，创建对应个数的 MessageQueue，这些队列都分布在 qd.getBrokerName() 的 Broker 上
                    // todo 新版支持 16 个，具体多少看路由信息中的值
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        // 1 对应所属的 topic
                        // 2 对应所属的 Broker
                        // 3 队列Id 使用序号
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMessageQueueList().add(mq);
                    }
                }
            }

            // 非有序消息
            info.setOrderTopic(false);
        }

        return info;
    }

    /**
     * Topic 路由信息转为订阅信息，创建 Topic 对应的读队列
     * @param topic
     * @param route
     * @return
     */
    public static Set<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic, final TopicRouteData route) {
        // 根据 TopicRouteData 信息创建 readQueueNums 条读消息队列
        Set<MessageQueue> mqList = new HashSet<MessageQueue>();

        // 获取 Topic 的队列信息
        List<QueueData> qds = route.getQueueDatas();

        // 遍历队列集合
        for (QueueData qd : qds) {

            // 是否具有读权限
            if (PermName.isReadable(qd.getPerm())) {

                // todo 有多少个读队列，就创建多少个读类型的 MessageQueue
                for (int i = 0; i < qd.getReadQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }

        return mqList;
    }

    /**
     * 启动客户端实例
     *
     * @throws MQClientException
     */
    public void start() throws MQClientException {

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    // 先将服务状态改为启动失败，但不是真的失败
                    this.serviceState = ServiceState.START_FAILED;
                    // If not specified,looking address from name server
                    if (null == this.clientConfig.getNamesrvAddr()) {
                        this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    // Start request-response channel
                    // todo 开启消息消费所需要的请求发送和响应通道
                    this.mQClientAPIImpl.start();

                    // Start various schedule tasks
                    // TODO 12.1 开启各种定时任务
                    this.startScheduledTask();


                    // Start pull service
                    // TODO 12.2 开启拉取消息的服务（消费者：线程）
                    // todo 注意：先启动拉取消息线程，等待拉取消息任务的到来，没有则阻塞，后负载均衡消息队列，均衡后就会为队列创建拉取消息任务
                    this.pullMessageService.start();
                    // Start rebalance service

                    // TODO 12.3 负载均衡服务（消费者：线程）
                    // todo 启动均衡消息队列任务 - 负载均衡服务线程-RebalanceService 的启动(每 20s 执行一次)
                    // todo 最终调用的是 org.apache.rocketmq.client.impl.consumer.RebalanceImpl#rebalanceByTopic 方法，实现 consumer 端负载均衡
                    this.rebalanceService.start();


                    // TODO 12.2 开启推送消息的服务（消费者：线程）
                    // 启动内部默认的生产者，用于消费者 SendMessageBack ，但不会执行 MQClientInstance.start()
                    // Start push service
                    this.defaultMQProducer.getDefaultMQProducerImpl().start(false);
                    log.info("the client factory [{}] start OK", this.clientId);

                    // 启动成功
                    this.serviceState = ServiceState.RUNNING;
                    break;
                case START_FAILED:
                    throw new MQClientException("The Factory object[" + this.getClientId() + "] has been created before, and failed.", null);
                default:
                    break;
            }
        }
    }

    /**
     * 开启各种任务
     */
    // todo 服务器出现宕机，生产者或消费者需要自己处理故障
    private void startScheduledTask() {
        // todo  没有配置 NameSrv ，2分钟拉取一次 NameSrv 地址
        if (null == this.clientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientInstance.this.mQClientAPIImpl.fetchNameServerAddr();
                    } catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }


        // todo 定时更新 Topic 路由，默认 30s 拉取一次 NameSrv 上的 Topic 路由信息并更新到本地
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.updateTopicRouteInfoFromNameServer();
                } catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 10, this.clientConfig.getPollNameServerInterval(), TimeUnit.MILLISECONDS);


        // todo 30s删除一次离线的Broker，并且生产者和消费者都会发送心跳给所有的Broker
        // todo Broker 可以知道客户端信息
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.cleanOfflineBroker();
                    MQClientInstance.this.sendHeartbeatToAllBrokerWithLock();
                } catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.clientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);

        // todo 默认消费端启动 10s 后 , 每隔 5s 持久化一次所有消费队列的偏移量(广播消费保存到本地，集群消费保存到 Broker上)
        // todo Broker 也会使用后台线程定时将消费进度写入文件或者写入本地文件，这对应使用的两种消费进度过滤
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.persistAllConsumerOffset();
                } catch (Exception e) {
                    log.error("ScheduledTask persistAllConsumerOffset exception", e);
                }
            }
        }, 1000 * 10, this.clientConfig.getPersistConsumerOffsetInterval(), TimeUnit.MILLISECONDS);

        // todo 1分钟调整一次 push消费模式下的消费者线程池
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientInstance.this.adjustThreadPool();
                } catch (Exception e) {
                    log.error("ScheduledTask adjustThreadPool exception", e);
                }
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public String getClientId() {
        return clientId;
    }

    /**
     * todo 从 NameSrv 更新 Topic 路由信息，具体如下：
     * 1. 如果是消费者，则获取订阅的数据，取出订阅的 Topic
     * 2. 如果是生产者，则获取 Topic 发布信息，取出对应的 Topic
     * 3. 根据 Topic 从 NameSrv 获取 Topic 的路由信息
     * todo 特别说明
     * 1. 如果是一台新加入的 Broker 机器，额外的什么都没有做，那么它就不能和其他 Broker 那样，拥有业务 Topic 信息(不考虑自动创建主题的情况)，NameSrv 中就不会有该 Broker 上报的 Topic 路由信息
     * 2. 如果在新加入的 Broker 上设置了业务 Topic 信息，那么就会上报到 NameSrv。关键的是：客户端会定时从 NameSrv 拉取感兴趣的 Topic 路由信息，更新到本地
     */
    public void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<String>();

        // Consumer
        {
            // 遍历当前 JVM 实例下的消费组下的消费者
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    // 获取消费端的订阅信息
                    // 注意：这里是订阅信息，不是转换的队列信息，这是合理的，订阅信息更能反映当前消费者订阅的 Topic
                    Set<SubscriptionData> subList = impl.subscriptions();
                    if (subList != null) {
                        // 遍历订阅的 Topic
                        for (SubscriptionData subData : subList) {
                            topicList.add(subData.getTopic());
                        }
                    }
                }
            }
        }

        // Producer
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    // 获取消息生产者缓存的 Topic 发布信息中的 Topic
                    Set<String> lst = impl.getPublishTopicList();
                    topicList.addAll(lst);
                }
            }
        }

        // todo 从 NameSrv 拉 Topic 的路由信息，并更新到当前 JVM 实例本地，具体来说就是更新消息生产者本地缓存 Topic 路由信息和消息消费者本地缓存 Topic 路由信息
        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }

    /**
     * @param offsetTable
     * @param namespace
     * @return newOffsetTable
     */
    public Map<MessageQueue, Long> parseOffsetTableFromBroker(Map<MessageQueue, Long> offsetTable, String namespace) {
        HashMap<MessageQueue, Long> newOffsetTable = new HashMap<MessageQueue, Long>();
        if (StringUtils.isNotEmpty(namespace)) {
            for (Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
                MessageQueue queue = entry.getKey();
                queue.setTopic(NamespaceUtil.withoutNamespace(queue.getTopic(), namespace));
                newOffsetTable.put(queue, entry.getValue());
            }
        } else {
            newOffsetTable.putAll(offsetTable);
        }

        return newOffsetTable;
    }

    /**
     * Remove offline broker
     */
    private void cleanOfflineBroker() {
        try {
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS))
                try {
                    ConcurrentHashMap<String, HashMap<Long, String>> updatedTable = new ConcurrentHashMap<String, HashMap<Long, String>>();

                    Iterator<Entry<String, HashMap<Long, String>>> itBrokerTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerTable.hasNext()) {
                        Entry<String, HashMap<Long, String>> entry = itBrokerTable.next();
                        String brokerName = entry.getKey();
                        HashMap<Long, String> oneTable = entry.getValue();

                        HashMap<Long, String> cloneAddrTable = new HashMap<Long, String>();
                        cloneAddrTable.putAll(oneTable);

                        Iterator<Entry<Long, String>> it = cloneAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> ee = it.next();
                            String addr = ee.getValue();
                            if (!this.isBrokerAddrExistInTopicRouteTable(addr)) {
                                it.remove();
                                log.info("the broker addr[{} {}] is offline, remove it", brokerName, addr);
                            }
                        }

                        if (cloneAddrTable.isEmpty()) {
                            itBrokerTable.remove();
                            log.info("the broker[{}] name's host is offline, remove it", brokerName);
                        } else {
                            updatedTable.put(brokerName, cloneAddrTable);
                        }
                    }

                    if (!updatedTable.isEmpty()) {
                        this.brokerAddrTable.putAll(updatedTable);
                    }
                } finally {
                    this.lockNamesrv.unlock();
                }
        } catch (InterruptedException e) {
            log.warn("cleanOfflineBroker Exception", e);
        }
    }

    public void checkClientInBroker() throws MQClientException {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();

        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            Set<SubscriptionData> subscriptionInner = entry.getValue().subscriptions();
            if (subscriptionInner == null || subscriptionInner.isEmpty()) {
                return;
            }

            for (SubscriptionData subscriptionData : subscriptionInner) {
                if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
                    continue;
                }
                // may need to check one broker every cluster...
                // assume that the configs of every broker in cluster are the the same.
                String addr = findBrokerAddrByTopic(subscriptionData.getTopic());

                if (addr != null) {
                    try {
                        this.getMQClientAPIImpl().checkClientInBroker(
                            addr, entry.getKey(), this.clientId, subscriptionData, 3 * 1000
                        );
                    } catch (Exception e) {
                        if (e instanceof MQClientException) {
                            throw (MQClientException) e;
                        } else {
                            throw new MQClientException("Check client in broker error, maybe because you use "
                                + subscriptionData.getExpressionType() + " to filter message, but server has not been upgraded to support!"
                                + "This error would not affect the launch of consumer, but may has impact on message receiving if you " +
                                "have use the new features which are not supported by server, please check the log!", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 发送心跳到所有的 Broker
     */
    public void sendHeartbeatToAllBrokerWithLock() {
        if (this.lockHeartbeat.tryLock()) {
            try {
                // 发送心跳到所有的 Broker
                this.sendHeartbeatToAllBroker();

                // 上报 classFilter 过滤信息到 Broker
                this.uploadFilterClassSource();
            } catch (final Exception e) {
                log.error("sendHeartbeatToAllBroker exception", e);
            } finally {
                this.lockHeartbeat.unlock();
            }
        } else {
            log.warn("lock heartBeat, but failed. [{}]", this.clientId);
        }
    }

    private void persistAllConsumerOffset() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            impl.persistConsumerOffset();
        }
    }

    public void adjustThreadPool() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> entry = it.next();
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    if (impl instanceof DefaultMQPushConsumerImpl) {
                        DefaultMQPushConsumerImpl dmq = (DefaultMQPushConsumerImpl) impl;
                        dmq.adjustThreadPool();
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * 从 NameSrv 更新 Topic 路由信息
     *
     * @param topic
     * @return
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        return updateTopicRouteInfoFromNameServer(topic, false, null);
    }

    private boolean isBrokerAddrExistInTopicRouteTable(final String addr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> entry = it.next();
            TopicRouteData topicRouteData = entry.getValue();
            List<BrokerData> bds = topicRouteData.getBrokerDatas();
            for (BrokerData bd : bds) {
                if (bd.getBrokerAddrs() != null) {
                    boolean exist = bd.getBrokerAddrs().containsValue(addr);
                    if (exist)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * 发送心跳到所有的 Broker
     */
    private void sendHeartbeatToAllBroker() {
        // 准备向 Broker 发送的心跳包
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();

        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending heartbeat, but no consumer and no producer. [{}]", this.clientId);
            return;
        }

        // 向所有的 Broker 发送心跳包
        if (!this.brokerAddrTable.isEmpty()) {
            long times = this.sendHeartbeatTimesTotal.getAndIncrement();
            Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, HashMap<Long, String>> entry = it.next();
                String brokerName = entry.getKey();
                HashMap<Long, String> oneTable = entry.getValue();
                if (oneTable != null) {
                    for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                        Long id = entry1.getKey();
                        String addr = entry1.getValue();
                        if (addr != null) {
                            // 如果消费者信息不存在 && Broker 不是主节点，则结束本次循环
                            if (consumerEmpty) {
                                if (id != MixAll.MASTER_ID)
                                    continue;
                            }

                            try {
                                int version = this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                                if (!this.brokerVersionTable.containsKey(brokerName)) {
                                    this.brokerVersionTable.put(brokerName, new HashMap<String, Integer>(4));
                                }
                                this.brokerVersionTable.get(brokerName).put(addr, version);
                                // todo 每 20 次打印一次发送心跳到 Broker 成功的日志
                                if (times % 20 == 0) {
                                    log.info("send heart beat to broker[{} {} {}] success", brokerName, id, addr);
                                    log.info(heartbeatData.toString());
                                }
                            } catch (Exception e) {
                                if (this.isBrokerInNameServer(addr)) {
                                    log.info("send heart beat to broker[{} {} {}] failed", brokerName, id, addr, e);
                                } else {
                                    log.info("send heart beat to broker[{} {} {}] exception, because the broker not up, forget it", brokerName,
                                        id, addr, e);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void uploadFilterClassSource() {
        Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, MQConsumerInner> next = it.next();
            MQConsumerInner consumer = next.getValue();
            if (ConsumeType.CONSUME_PASSIVELY == consumer.consumeType()) {
                Set<SubscriptionData> subscriptions = consumer.subscriptions();
                for (SubscriptionData sub : subscriptions) {
                    if (sub.isClassFilterMode() && sub.getFilterClassSource() != null) {
                        final String consumerGroup = consumer.groupName();
                        final String className = sub.getSubString();
                        final String topic = sub.getTopic();
                        final String filterClassSource = sub.getFilterClassSource();
                        try {
                            this.uploadFilterClassToAllFilterServer(consumerGroup, className, topic, filterClassSource);
                        } catch (Exception e) {
                            log.error("uploadFilterClassToAllFilterServer Exception", e);
                        }
                    }
                }
            }
        }
    }

    /**
     * 尝试从 NameSrv 获取配置信息并更新到本地，实例中维护的 TopicRouteTable 和 实例具体的生产者/消费者的队列信息
     * 根据 Topic 从 NameSrv 获取 topic 的路由信息
     * todo 特别说明：
     * 1. 在 Broker 启动时或运行期间，会定期向每个 NameSrv 上报自身信息以及 Topic 信息
     * 2. 在 Broker 启动时会初始化一些固定的 Topic 信息，比如 TBW102，然后向 NameSrv 上报，具体参与： {@link org.apache.rocketmq.broker.BrokerController#start())
     * 3. 因此该方法获取 TBW102 的路由信息一定可以获取到的，因为 Broker启动时默认创建的，Broker启动时会向 NameSrv 注册；
     * 4. 生产者发送消息时，指定的 Topic 在首次发送消息的时候，从 NameSrv 是拉取不到的，因为它压根没有上报过 NameSrv
     * 5. todo NameSrv 请求异常不会更新缓存，即使 NameSrv 全部宕机了，只要有缓存就可以请求 Broker，额外需要注意，请求某个 NameSrv 异常后，下次会请求其他的 NameSrv
     *
     * @param topic
     * @param isDefault
     * @param defaultMQProducer
     * @return
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic, boolean isDefault,
        DefaultMQProducer defaultMQProducer) {
        try {
            // 给 NameServer 加锁，进行 TopicRoteInfo 拉取
            // 避免重复从 NameSrv 拉取配置信息，在这里使用了 ReentrantLock
            if (this.lockNamesrv.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData;

                    // todo 将入参的 Topic 转换为默认的 TBW102 , 获取 TBW102 的信息
                    // 获取默认的 Topic 的配置信息，通过与 NameSrv 的长连接 Channel 发送 RequestCode.GET_ROUTEINFO_BY_TOPIC 命令获取配置信息
                    if (isDefault && defaultMQProducer != null) {
                        // 默认主题 TBW102 下的队列数
                        topicRouteData = this.mQClientAPIImpl.getDefaultTopicRouteInfoFromNameServer(defaultMQProducer.getCreateTopicKey(),
                            1000 * 3);

                        // todo 获取成功，修正读写队列数，以发送者配置的默认队列数为准
                        if (topicRouteData != null) {
                            // todo 获取队列信息
                            for (QueueData data : topicRouteData.getQueueDatas()) {

                                // todo 根据生产者默认队列数，即读写队列最大值，默认 4 个，更新队列数目信息
                                int queueNums = Math.min(defaultMQProducer.getDefaultTopicQueueNums(), data.getReadQueueNums());
                                data.setReadQueueNums(queueNums);
                                data.setWriteQueueNums(queueNums);
                            }
                        }

                        // 根据 Topic 从 nameServer 中获取 TopicRouteInfo 信息;通过 NameSrv 的长连接 Channel 发送 RequestCode.GET_ROUTEINFO_BY_TOPIC 命令获取配置信息
                    } else {
                        topicRouteData = this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    }

                    // 拿到最新的 Topic 路由信息后，需要与客户端实例本地缓存中的 Topic 路由进行比较，如果有变化，则需要同步更新生产者、消费者关于该 Topic 的缓存
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        // 对比新老 TopicRouteData 的信息：brokerDatas、orderTopicInfo、queueDatas、filterServerTable
                        boolean changed = topicRouteDataIsChange(old, topicRouteData);

                        // 相等，再判断当前 JVM 实例下的生产者或消费者中缓存的该 Topic 的发布信息是否改变或可用
                        if (!changed) {
                            // 没有变化，判断 Topic 在 topicPublishInfoTable 和 rebalanceImpl#topicSubscribeInfoTable 中是否存在，
                            // 存在-》changed = false ，不存在-》changed = true
                            changed = this.isNeedUpdateTopicRouteInfo(topic);
                        } else {
                            log.info("the topic[{}] route info changed, old[{}] ,new[{}]", topic, old, topicRouteData);
                        }

                        //--------------------todo 同步更新 消息生产者、消息消费者关于该 Topic 的缓存(特别读写队列的创建)，后续可以直接从缓存查找
                        if (changed) {
                            // 尝试更新当前 JVM 实例中消息生产者的本地关于当前 Topic 的发布信息缓存
                            // 将最新的 TopicRouteData 克隆一份，保存到 topicRouteTable 中，便于后续 TopicRouteData 信息是否变更的判断
                            TopicRouteData cloneTopicRouteData = topicRouteData.cloneTopicRouteData();
                            // 添加/更新 MQ客户端实例中的 brokerAddrTable 信息
                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // 1. todo update pub info 将 Topic 的 Route 信息转换为 publish 信息
                            // 如果开启自动创建 Topic 功能，则实际是用了 TBW102 的 Route 信息，给我们指定的 Topic 用，即我们指定的 Topic 继承了 TBW102 的路由信息。因此我们指定的 Topic 的发布信息就有了
                            {
                                // Topic 路由信息转换为 Topic 发布消息
                                // todo 创建 Broker 用于存储消息的写队列
                                TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                                // 标记下，表示有 Topic 路由信息
                                publishInfo.setHaveTopicRouterInfo(true);

                                // 遍历生产者信息，并尝试更新 Topic 发布信息
                                Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQProducerInner> entry = it.next();
                                    MQProducerInner impl = entry.getValue();
                                    if (impl != null) {
                                        // todo 更新生产者实例缓存的 Topic 发布信息到本地，注意其中是写队列
                                        impl.updateTopicPublishInfo(topic, publishInfo);
                                    }
                                }
                            }

                            // 2. todo Update sub info 更新消费者的缓存(订阅队列信息)
                            {
                                // todo ，获取消息队列 - 具有读权限的队列
                                Set<MessageQueue> subscribeInfo = topicRouteData2TopicSubscribeInfo(topic, topicRouteData);

                                // 遍历消费者信息
                                Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
                                while (it.hasNext()) {
                                    Entry<String, MQConsumerInner> entry = it.next();
                                    MQConsumerInner impl = entry.getValue();
                                    if (impl != null) {
                                        // todo 更新消费者实例缓存的 Topic 订阅信息到本地，注意其中是读队列
                                        impl.updateTopicSubscribeInfo(topic, subscribeInfo);
                                    }
                                }
                            }
                            log.info("topicRouteTable.put. Topic = {}, TopicRouteData[{}]", topic, cloneTopicRouteData);

                            // 缓存路由信息
                            this.topicRouteTable.put(topic, cloneTopicRouteData);
                            return true;
                        }
                    } else {
                        log.warn("updateTopicRouteInfoFromNameServer, getTopicRouteInfoFromNameServer return null, Topic: {}. [{}]", topic, this.clientId);
                    }
                } catch (MQClientException e) {
                    if (!topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        log.warn("updateTopicRouteInfoFromNameServer Exception", e);
                    }
                } catch (RemotingException e) {
                    log.error("updateTopicRouteInfoFromNameServer Exception", e);
                    throw new IllegalStateException(e);
                } finally {
                    this.lockNamesrv.unlock();
                }
            } else {
                log.warn("updateTopicRouteInfoFromNameServer tryLock timeout {}ms. [{}]", LOCK_TIMEOUT_MILLIS, this.clientId);
            }
        } catch (InterruptedException e) {
            log.warn("updateTopicRouteInfoFromNameServer Exception", e);
        }

        return false;
    }

    /**
     * 准备生产者和消费者向 Broker 发送的心跳包
     *
     * @return
     */
    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID 客户端实例Id
        heartbeatData.setClientID(this.clientId);

        // Consumer 消费者心跳数据包
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.groupName());
                consumerData.setConsumeType(impl.consumeType());
                consumerData.setMessageModel(impl.messageModel());
                consumerData.setConsumeFromWhere(impl.consumeFromWhere());

                // 消费者的订阅信息 -- 由订阅表达式解析而来
                consumerData.getSubscriptionDataSet().addAll(impl.subscriptions());
                consumerData.setUnitMode(impl.isUnitMode());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // Producer 生产者心跳数据包
        for (Map.Entry<String/* group */, MQProducerInner> entry : this.producerTable.entrySet()) {
            MQProducerInner impl = entry.getValue();
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(entry.getKey());

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }

    private boolean isBrokerInNameServer(final String brokerAddr) {
        Iterator<Entry<String, TopicRouteData>> it = this.topicRouteTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, TopicRouteData> itNext = it.next();
            List<BrokerData> brokerDatas = itNext.getValue().getBrokerDatas();
            for (BrokerData bd : brokerDatas) {
                boolean contain = bd.getBrokerAddrs().containsValue(brokerAddr);
                if (contain)
                    return true;
            }
        }

        return false;
    }

    /**
     * This method will be removed in the version 5.0.0,because filterServer was removed,and method
     * <code>subscribe(final String topic, final MessageSelector messageSelector)</code> is recommended.
     */
    @Deprecated
    private void uploadFilterClassToAllFilterServer(final String consumerGroup, final String fullClassName,
        final String topic,
        final String filterClassSource) throws UnsupportedEncodingException {
        byte[] classBody = null;
        int classCRC = 0;
        try {
            classBody = filterClassSource.getBytes(MixAll.DEFAULT_CHARSET);
            classCRC = UtilAll.crc32(classBody);
        } catch (Exception e1) {
            log.warn("uploadFilterClassToAllFilterServer Exception, ClassName: {} {}",
                fullClassName,
                RemotingHelper.exceptionSimpleDesc(e1));
        }

        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null
            && topicRouteData.getFilterServerTable() != null && !topicRouteData.getFilterServerTable().isEmpty()) {
            Iterator<Entry<String, List<String>>> it = topicRouteData.getFilterServerTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<String>> next = it.next();
                List<String> value = next.getValue();
                for (final String fsAddr : value) {
                    try {
                        this.mQClientAPIImpl.registerMessageFilterClass(fsAddr, consumerGroup, topic, fullClassName, classCRC, classBody,
                            5000);

                        log.info("register message class filter to {} OK, ConsumerGroup: {} Topic: {} ClassName: {}", fsAddr, consumerGroup,
                            topic, fullClassName);

                    } catch (Exception e) {
                        log.error("uploadFilterClassToAllFilterServer Exception", e);
                    }
                }
            }
        } else {
            log.warn("register message class filter failed, because no filter server, ConsumerGroup: {} Topic: {} ClassName: {}",
                consumerGroup, topic, fullClassName);
        }
    }

    /**
     * 比对路由信息是否改变，对比 Topic 的队列和这些队列所在 Broker 分布情况是否发送改变
     *
     * @param olddata
     * @param nowdata
     * @return 返回发生改变的判断结果(不相等)
     */
    private boolean topicRouteDataIsChange(TopicRouteData olddata, TopicRouteData nowdata) {
        if (olddata == null || nowdata == null)
            return true;
        TopicRouteData old = olddata.cloneTopicRouteData();
        TopicRouteData now = nowdata.cloneTopicRouteData();
        Collections.sort(old.getQueueDatas());
        Collections.sort(old.getBrokerDatas());
        Collections.sort(now.getQueueDatas());
        Collections.sort(now.getBrokerDatas());
        return !old.equals(now);

    }

    /**
     * 是否更新当前实例下缓存的生产者的 Topic 路由信息，消费者订阅的消息队列
     * todo 依据：
     * 根据 Topic 获取缓存在生产者或消费者内存中的 Topic 的发布消息，看是否存在或可用，以此作为判断条件
     *
     * @param topic
     * @return
     */
    private boolean isNeedUpdateTopicRouteInfo(final String topic) {
        boolean result = false;
        {
            Iterator<Entry<String, MQProducerInner>> it = this.producerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQProducerInner> entry = it.next();
                MQProducerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isPublishTopicNeedUpdate(topic);
                }
            }
        }

        {
            Iterator<Entry<String, MQConsumerInner>> it = this.consumerTable.entrySet().iterator();
            while (it.hasNext() && !result) {
                Entry<String, MQConsumerInner> entry = it.next();
                MQConsumerInner impl = entry.getValue();
                if (impl != null) {
                    result = impl.isSubscribeTopicNeedUpdate(topic);
                }
            }
        }

        return result;
    }

    public void shutdown() {
        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        // AdminExt
        if (!this.adminExtTable.isEmpty())
            return;

        // Producer
        if (this.producerTable.size() > 1)
            return;

        synchronized (this) {
            switch (this.serviceState) {
                case CREATE_JUST:
                    break;
                case RUNNING:
                    this.defaultMQProducer.getDefaultMQProducerImpl().shutdown(false);

                    this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                    this.pullMessageService.shutdown(true);
                    this.scheduledExecutorService.shutdown();
                    this.mQClientAPIImpl.shutdown();
                    this.rebalanceService.shutdown();

                    MQClientManager.getInstance().removeClientFactory(this.clientId);
                    log.info("the client factory [{}] shutdown OK", this.clientId);
                    break;
                case SHUTDOWN_ALREADY:
                    break;
                default:
                    break;
            }
        }
    }

    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }

    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
        this.unregisterClientWithLock(null, group);
    }

    private void unregisterClientWithLock(final String producerGroup, final String consumerGroup) {
        try {
            if (this.lockHeartbeat.tryLock(LOCK_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                try {
                    this.unregisterClient(producerGroup, consumerGroup);
                } catch (Exception e) {
                    log.error("unregisterClient exception", e);
                } finally {
                    this.lockHeartbeat.unlock();
                }
            } else {
                log.warn("lock heartBeat, but failed. [{}]", this.clientId);
            }
        } catch (InterruptedException e) {
            log.warn("unregisterClientWithLock exception", e);
        }
    }

    private void unregisterClient(final String producerGroup, final String consumerGroup) {
        Iterator<Entry<String, HashMap<Long, String>>> it = this.brokerAddrTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, HashMap<Long, String>> entry = it.next();
            String brokerName = entry.getKey();
            HashMap<Long, String> oneTable = entry.getValue();

            if (oneTable != null) {
                for (Map.Entry<Long, String> entry1 : oneTable.entrySet()) {
                    String addr = entry1.getValue();
                    if (addr != null) {
                        try {
                            this.mQClientAPIImpl.unregisterClient(addr, this.clientId, producerGroup, consumerGroup, 3000);
                            log.info("unregister client[Producer: {} Consumer: {}] from broker[{} {} {}] success", producerGroup, consumerGroup, brokerName, entry1.getKey(), addr);
                        } catch (RemotingException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (InterruptedException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        } catch (MQBrokerException e) {
                            log.error("unregister client exception from broker: " + addr, e);
                        }
                    }
                }
            }
        }
    }

    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        MQProducerInner prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
        this.unregisterClientWithLock(group, null);
    }

    public boolean registerAdminExt(final String group, final MQAdminExtInner admin) {
        if (null == group || null == admin) {
            return false;
        }

        MQAdminExtInner prev = this.adminExtTable.putIfAbsent(group, admin);
        if (prev != null) {
            log.warn("the admin group[{}] exist already.", group);
            return false;
        }

        return true;
    }

    public void unregisterAdminExt(final String group) {
        this.adminExtTable.remove(group);
    }

    public void rebalanceImmediately() {
        this.rebalanceService.wakeup();
    }

    /**
     * 为 Consumer 分配队列，todo 这是根据消费组来分配的
     */
    public void doRebalance() {
        // todo 遍历当前 Client 包含的 consumer 集合，以消费组维度为每个消费者执行消息队列分配
        for (Map.Entry<String, MQConsumerInner> entry : this.consumerTable.entrySet()) {
            // 消费者
            MQConsumerInner impl = entry.getValue();
            if (impl != null) {
                try {
                    // TODO 12.3.3 负载均衡方法：org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl.doRebalance
                    /**
                     * 调用 MQConsumerInner#doRebalance(....) 进行队列分配
                     * 说明：
                     * DefaultMQPushConsumerImpl、DefaultMQPullConsumerImpl 分别对该接口方法进行了实现
                     */
                    impl.doRebalance();
                } catch (Throwable e) {
                    log.error("doRebalance exception", e);
                }
            }
        }
    }

    public MQProducerInner selectProducer(final String group) {
        return this.producerTable.get(group);
    }

    public MQConsumerInner selectConsumer(final String group) {
        return this.consumerTable.get(group);
    }

    /**
     * 根据 BrokerName 查找对应 Broker 地址，优先主服务
     * @param brokerName
     * @return
     */
    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                // 找到即返回
                if (brokerAddr != null) {
                    found = true;

                    // 优先主服务器
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                    } else {
                        slave = true;
                    }
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    /**
     * 根据 Broker Name 获取 Broker 主节点地址
     *
     * @param brokerName
     * @return
     */
    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            // 获取 Broker 主节点地址
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }

    /**
     * 获取 Broker 信息
     *
     * @param brokerName     Broker 名字
     * @param brokerId       Broker Id
     * @param onlyThisBroker 是否必须返回 BrokerId 的 Broker 信息
     * @return
     */
    public FindBrokerResult findBrokerAddressInSubscribe(
        final String brokerName,
        final long brokerId,
        final boolean onlyThisBroker
    ) {
        // Broker 地址
        String brokerAddr = null;
        // 是否为从节点
        boolean slave = false;
        // 是否找到
        boolean found = false;

        // 根据 BrokerName 获取所有的 Broker 信息
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            // 根据 BrokerId 获取 Broker 地址
            brokerAddr = map.get(brokerId);
            slave = brokerId != MixAll.MASTER_ID;
            found = brokerAddr != null;

            // 如果在当前从节点中，没有找到，则顺延从下一个从节点获取
            if (!found && slave) {
                brokerAddr = map.get(brokerId + 1);
                found = brokerAddr != null;
            }

            // 如果不强制获得，选择一个 Broker
            if (!found && !onlyThisBroker) {
                Entry<Long, String> entry = map.entrySet().iterator().next();
                brokerAddr = entry.getValue();
                slave = entry.getKey() != MixAll.MASTER_ID;
                found = true;
            }
        }

        // 找到 Broker ，则返回信息
        if (found) {
            return new FindBrokerResult(brokerAddr, slave, findBrokerVersion(brokerName, brokerAddr));
        }

        return null;
    }

    public int findBrokerVersion(String brokerName, String brokerAddr) {
        if (this.brokerVersionTable.containsKey(brokerName)) {
            if (this.brokerVersionTable.get(brokerName).containsKey(brokerAddr)) {
                return this.brokerVersionTable.get(brokerName).get(brokerAddr);
            }
        }
        //To do need to fresh the version
        return 0;
    }

    /**
     * 根据 Group 获取下面所有的消费者Id
     *
     * @param topic
     * @param group
     * @return
     */
    public List<String> findConsumerIdList(final String topic, final String group) {
        // 根据 Topic 随机获取对应的一个 Broker 地址
        String brokerAddr = this.findBrokerAddrByTopic(topic);
        if (null == brokerAddr) {
            this.updateTopicRouteInfoFromNameServer(topic);
            brokerAddr = this.findBrokerAddrByTopic(topic);
        }

        if (null != brokerAddr) {
            try {
                // 获取消费组下的所有消费者
                return this.mQClientAPIImpl.getConsumerIdListByGroup(brokerAddr, group, 3000);
            } catch (Exception e) {
                log.warn("getConsumerIdListByGroup exception, " + brokerAddr + " " + group, e);
            }
        }

        return null;
    }

    /**
     * 根据 Topic 随机获取对应的一个 Broker 地址
     *
     * @param topic
     * @return
     */
    public String findBrokerAddrByTopic(final String topic) {
        TopicRouteData topicRouteData = this.topicRouteTable.get(topic);
        if (topicRouteData != null) {
            List<BrokerData> brokers = topicRouteData.getBrokerDatas();
            if (!brokers.isEmpty()) {
                int index = random.nextInt(brokers.size());
                BrokerData bd = brokers.get(index % brokers.size());
                return bd.selectBrokerAddr();
            }
        }

        return null;
    }

    public synchronized void resetOffset(String topic, String group, Map<MessageQueue, Long> offsetTable) {
        DefaultMQPushConsumerImpl consumer = null;
        try {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
                consumer = (DefaultMQPushConsumerImpl) impl;
            } else {
                log.info("[reset-offset] consumer dose not exist. group={}", group);
                return;
            }
            consumer.suspend();

            ConcurrentMap<MessageQueue, ProcessQueue> processQueueTable = consumer.getRebalanceImpl().getProcessQueueTable();
            for (Map.Entry<MessageQueue, ProcessQueue> entry : processQueueTable.entrySet()) {
                MessageQueue mq = entry.getKey();
                if (topic.equals(mq.getTopic()) && offsetTable.containsKey(mq)) {
                    ProcessQueue pq = entry.getValue();
                    pq.setDropped(true);
                    pq.clear();
                }
            }

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
            }

            Iterator<MessageQueue> iterator = processQueueTable.keySet().iterator();
            while (iterator.hasNext()) {
                MessageQueue mq = iterator.next();
                Long offset = offsetTable.get(mq);
                if (topic.equals(mq.getTopic()) && offset != null) {
                    try {
                        consumer.updateConsumeOffset(mq, offset);
                        consumer.getRebalanceImpl().removeUnnecessaryMessageQueue(mq, processQueueTable.get(mq));
                        iterator.remove();
                    } catch (Exception e) {
                        log.warn("reset offset failed. group={}, {}", group, mq, e);
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.resume();
            }
        }
    }

    public Map<MessageQueue, Long> getConsumerStatus(String topic, String group) {
        MQConsumerInner impl = this.consumerTable.get(group);
        if (impl != null && impl instanceof DefaultMQPushConsumerImpl) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else if (impl != null && impl instanceof DefaultMQPullConsumerImpl) {
            DefaultMQPullConsumerImpl consumer = (DefaultMQPullConsumerImpl) impl;
            return consumer.getOffsetStore().cloneOffsetTable(topic);
        } else {
            return Collections.EMPTY_MAP;
        }
    }

    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }

    public MQClientAPIImpl getMQClientAPIImpl() {
        return mQClientAPIImpl;
    }

    public MQAdminImpl getMQAdminImpl() {
        return mQAdminImpl;
    }

    public long getBootTimestamp() {
        return bootTimestamp;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public PullMessageService getPullMessageService() {
        return pullMessageService;
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    public ConcurrentMap<String, TopicRouteData> getTopicRouteTable() {
        return topicRouteTable;
    }

    public ConsumeMessageDirectlyResult consumeMessageDirectly(final MessageExt msg,
        final String consumerGroup,
        final String brokerName) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (null != mqConsumerInner) {
            DefaultMQPushConsumerImpl consumer = (DefaultMQPushConsumerImpl) mqConsumerInner;

            ConsumeMessageDirectlyResult result = consumer.getConsumeMessageService().consumeMessageDirectly(msg, brokerName);
            return result;
        }

        return null;
    }

    public ConsumerRunningInfo consumerRunningInfo(final String consumerGroup) {
        MQConsumerInner mqConsumerInner = this.consumerTable.get(consumerGroup);
        if (mqConsumerInner == null) {
            return null;
        }

        ConsumerRunningInfo consumerRunningInfo = mqConsumerInner.consumerRunningInfo();

        List<String> nsList = this.mQClientAPIImpl.getRemotingClient().getNameServerAddressList();

        StringBuilder strBuilder = new StringBuilder();
        if (nsList != null) {
            for (String addr : nsList) {
                strBuilder.append(addr).append(";");
            }
        }

        String nsAddr = strBuilder.toString();
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_NAMESERVER_ADDR, nsAddr);
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CONSUME_TYPE, mqConsumerInner.consumeType().name());
        consumerRunningInfo.getProperties().put(ConsumerRunningInfo.PROP_CLIENT_VERSION,
            MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));

        return consumerRunningInfo;
    }

    public ConsumerStatsManager getConsumerStatsManager() {
        return consumerStatsManager;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }
}
