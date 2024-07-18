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
package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * NameSrv 数据的载体，记录 Broker 、Topic 等信息，注意：这些信息都是保存在内存中的，并且没有持久化
 * 即 NameSrv 的主要功能：Broker 管理和 Topic 路由信息的管理，即保存了集群所有的 Broker 和 Topic 的路由信息
 * <p>
 * todo 特别说明：
 *  1. RocketMQ 中的路由信息是持久化在 Broker 上的，NameSrv的路由信息来源于 Broker 的心跳包，是存储在内存中的
 *  2. Broker 上的路由信息是基本的 Topic 和其队列配置信息，而 NameSrv 中的路由信息是 Broker 和 Topic 即队列的集合，毕竟上报信息是某个 Broker 的
 *  3. 客户端(生产者、消费者)缓存的路由发布信息格式<Topic,List<MessageQueue>>，每个 MessageQueue 包含队列的基本信息：
 *     Topic 、BrokerName、queueId
 *  4. 在 NameSrv 的 RouteInfoManager 类中，主要的路由信息就是 topicQueueTable 和 BrokerAddrTable 这两个 Map 来保存的
 *  5. 在 RouteInfoManage 中，这 5 个 Map 作为一个整体资源，使用了一个读写锁来控制并发，避免并发更新和更新过程中造成的读写不一致问题
 *  todo 客户端感觉路由信息发生变化需要的时间？
 *  - NameSrv 与 Broker 的 TCP 连接断开，此时 NameServer 能立即感知路由信息变化，将其从路由列表中删除，从而生产者应该在 30s 左右就能感知到路由发生变化，
 *    在此 30s 内，生产者会出现消息发送失败的情况，但是结合故障规避机制，并不会对发送方带来重大故障，可以接受
 *  - NameSrv 与 Broker 的 TCP 连接未断开，但是 Broker 已无法提供服务(假如假死)，此时 NameServer 需要 120s(定时任务重判定 Broker 异常的时间)，才能感知到 Broker 宕机，
 *    此时生产者最多需要 150s 才能感知其路由信息的变化
 */
public class RouteInfoManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /**
     * NameServer 与 Broker 连接的空闲时长，默认 2 分钟即 120s，在 2 分钟内 NameServer 没有收到 Broker 的心跳包，就关闭该链接
     */
    private final static long BROKER_CHANNEL_EXPIRED_TIME = 1000 * 60 * 2;

    /**
     * 读写锁，控制下列各个map在并发读写下的安全性
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * 存放 Topic 的队列信息
     * 1. topic 与 MessageQueue 的关系，记录一个主题的队列都分布在哪些 Broker 上，每个 Broker 上存在该主题的队列个数
     * 2. todo 每个队列信息对应的类 QueueData 中，保存了 BrokerName，但是请注意：这个 BrokerName，并不是 Broker 真正的物理地址，
     *     它对应的是一组 Broker 节点，包含一个主节点及多个从节点
     */
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;

    /**
     * 存放集群中 Broker 们的信息
     * 所有 Broker 信息，使用 BrokerName 作为 key，BrokerData 信息描述每个 Broker 信息
     * <p>
     * todo 通过 {@link topicQueueTable} 可以间接获取 brokerAddrs
     */
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;

    /**
     * Broker 集群信息，每个集群包含哪些 Broker
     * key-broker集群名称，value-对应集群中所有Broker名称
     */
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    /**
     * 当前存活的 Broker 信息，该信息不是实时的，NameServer 每隔 10s 扫描一次所有的 Broker，根据心跳包的时间得知 Broker 的状态
     * 该机制也是导致当一个 Broker 进程假死后，消息生产者无法立即感知，可能继续向该 Broker 发送消息，导致失败(非高可用)
     */
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

    /**
     * 保存了每个 Broker 对应的消息过滤服务的地址，用于 Broker端的消息过滤
     * Broker地址及对应的 Filter Server列表
     */
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTable = new HashMap<String, BrokerLiveInfo>(256);
        this.filterServerTable = new HashMap<String, List<String>>(256);
    }

    public byte[] getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper.encode();
    }

    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }

    // 生产者发送消息，需要频繁的获取。对表进行读
    public byte[] getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                // 获取路由信息时，使用读锁，确保消息发送时的高并发
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    // 因为 Broker 每隔30s 向 NameServer 发送一个心跳包，这个操作每次都会更新Broker的状态，但是同时生产者发送消息时，也需要Broker的状态，要进行频繁的读取操作。
    // 所以，这里就有一个矛盾的地方，Broker的状态，经常被更新，同时也被更加频繁的读取。这里如何提高并发，尤其是生产者进行消息发送时的并发。
    // 所以，这里使用了读写锁机制(针对读多写少的场景)
    // 为啥不用 synchronized 和 reentrantLock ?
    //
    // 注册 Broker 的核心方法，NameServer 主要做服务的注册与发现

    /**
     * 根据 Broker 请求过来的路由信息，
     * 依次对比更新 clusterAddrTable、brokerAddrTable、topicQueueTable、brokerLiveTable、filterServerTable
     * 这五个保存集群信息和路由信息的 Map 对象中的数据。
     * <p>
     * 为啥不用 synchronized 和 reentrantLock ?
     * 为了提高消息处理的吞吐量，才使用 ReadWriteLock(读写锁)，因为 NameSrv 上的数据对应的操作更多的是读取。
     * <p>
     * 因为 Broker 每隔 30s 向 NameServer 发送一个心跳包，这个操作每次都会更新Broker的状态，但是同时生产者发送消息时，也需要Broker的状态，要进行频繁的读取操作。
     * 所以，这里就有一个矛盾的地方，Broker 的状态，经常被更新，同时也被更加频繁的读取。这里如何提高并发，尤其是生产者进行消息发送时的并发。
     * 所以，这里使用了读写锁机制(针对读多写少的场景)
     *
     * @param clusterName        broker 集群名
     * @param brokerAddr         Broker 地址
     * @param brokerName         Broker 名称
     * @param brokerId           Broker Id  0:Master >0:Slave
     * @param haServerAddr
     * @param topicConfigWrapper Topic 包装数据
     * @param filterServerList   过滤器
     * @param channel
     * @return
     */
    public RegisterBrokerResult registerBroker(final String clusterName, final String brokerAddr, final String brokerName, final long brokerId, final String haServerAddr, final TopicConfigSerializeWrapper topicConfigWrapper, final List<String> filterServerList, final Channel channel) {
        RegisterBrokerResult result = new RegisterBrokerResult();
        try {
            try {
                // 注册路由信息时，使用写锁，防止并发时修改数据
                this.lock.writeLock().lockInterruptibly();

                // 1. 处理 Broker 集群信息，维护 clusterAddrTable 内容( Broker 集群信息，key-broker集群名称，value-对应集群中所有Broker名称)
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (null == brokerNames) {
                    brokerNames = new HashSet<String>();
                    this.clusterAddrTable.put(clusterName, brokerNames);
                }
                brokerNames.add(brokerName);

                // Broker 首次注册标识
                boolean registerFirst = false;

                // 2. 处理 Broker
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    // 修改首次注册标识为 true
                    registerFirst = true;
                    brokerData = new BrokerData(clusterName, brokerName, new HashMap<Long, String>());
                    this.brokerAddrTable.put(brokerName, brokerData);
                }
                Map<Long, String> brokerAddrsMap = brokerData.getBrokerAddrs();

                //Switch slave to master: first remove <1, IP:PORT> in namesrv, then add <0, IP:PORT>
                //The same IP:PORT must only have one record in brokerAddrTable

                // 去除 brokerAddrs 中与此次注册的 Broker 地址相同，但是 BrokerId 不同的数据
                Iterator<Entry<Long, String>> it = brokerAddrsMap.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<Long, String> item = it.next();
                    if (null != brokerAddr && brokerAddr.equals(item.getValue()) && brokerId != item.getKey()) {
                        it.remove();
                    }
                }

                // 如果是新注册的Broker 或者 Broker 中的路由信息发生改变了，需要更新 TopicQueueTable
                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                registerFirst = registerFirst || (null == oldAddr);

                // todo 首先必须保证传递过来的 Broker 是主节点，并且 Topic 包装数据不为空
                if (null != topicConfigWrapper && MixAll.MASTER_ID == brokerId) {
                    // todo Broker 配置改变(新注册的 Broker || Broker上的 Topic 路由信息发生改变了)，那么更新 TopicQueueTable
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion()) || registerFirst) {
                        // 获取 Topic 的配置信息
                        ConcurrentMap<String, TopicConfig> tcTable = topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            // 遍历 Topic 的配置信息，保存或修改 NameSrv 上的 TopicQueueTable 信息
                            for (Map.Entry<String, TopicConfig> entry : tcTable.entrySet()) {
                                // todo 创建或更新消息队列，这里以传过来的 BrokerName 作为消息队列所属的 Broker
                                this.createAndUpdateQueueData(brokerName, entry.getValue());
                            }
                        }
                    }
                }

                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr, new BrokerLiveInfo(System.currentTimeMillis(), topicConfigWrapper.getDataVersion(), channel, haServerAddr));
                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registered, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                if (MixAll.MASTER_ID != brokerId) {
                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }

        return result;
    }

    public boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        DataVersion prev = queryBrokerTopicConfig(brokerAddr);
        return null == prev || !prev.equals(dataVersion);
    }

    public DataVersion queryBrokerTopicConfig(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            return prev.getDataVersion();
        }
        return null;
    }

    public void updateBrokerInfoUpdateTimestamp(final String brokerAddr) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (prev != null) {
            prev.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    /**
     * 保存或更新 Topic-MessageQueue的对应关系
     * todo 特别说明：
     *  1. 组装 Broker 和 Topic 配置，最终得到的就是 TopicQueueTable 表
     *  2. 根据 Broker 的发送请求和上报的 Topic 配置信息，封装 Topic 的路由信息，信息内容包括：
     *     - Broker 元数据
     *     - Topic 的队列元数据
     *
     * @param brokerName
     * @param topicConfig
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        // 创建队列数据
        QueueData queueData = new QueueData();
        // todo 消息队列归属的 Broker
        queueData.setBrokerName(brokerName);
        // todo 写队列数量，用于生产者客户端在从 NameSrv 拉取 Topic 路由信息时，转为 写队列即(Write MessageQueue)
        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        // todo 读队列数量，用于消费者客户端在从 NameSrv 拉取 Topic 路由信息时，转为 读队列即(Read MessageQueue)
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());
        // todo Topic 队列的读写权限
        queueData.setPerm(topicConfig.getPerm());
        // todo Topic 的系统标识
        queueData.setTopicSysFlag(topicConfig.getTopicSysFlag());

        // todo 同一个 Topic 的消息队列，可能会分布到不同的 Broker 上，先看看该 Topic 是否存在对应的 QueueData 集合
        List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
        // NameSrv 上不存在该 Topic 对应的 QueueData ，直接存储
        if (null == queueDataList) {
            queueDataList = new LinkedList<QueueData>();
            queueDataList.add(queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
            log.info("new topic registered, {} {}", topicConfig.getTopicName(), queueData);

            // NameSrv 上存在该 topic 之前存储的 QueueData 集合
        } else {
            boolean addNewOne = true;

            // 遍历该 Topic 之前存储的 QueueData 集合
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                // 缓存中的 Broker 名称 和 该 Topic 归属的 Broker 名称相同
                if (qd.getBrokerName().equals(brokerName)) {
                    // 缓存中的 QueueData 和该 Topic 的 QueueData 相同，则不需要添加
                    // 注意：此处的 equals() 方法被重写了
                    if (qd.equals(queueData)) {
                        addNewOne = false;

                        // QueueData 不同，说明 QueueData 无效了，则移除
                    } else {
                        log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd, queueData);
                        it.remove();
                    }
                }
            }

            if (addNewOne) {
                queueDataList.add(queueData);
            }
        }
    }

    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return wipeWritePermOfBroker(brokerName);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }

    private int wipeWritePermOfBroker(final String brokerName) {
        int wipeTopicCnt = 0;
        Iterator<Entry<String, List<QueueData>>> itTopic = this.topicQueueTable.entrySet().iterator();
        while (itTopic.hasNext()) {
            Entry<String, List<QueueData>> entry = itTopic.next();
            List<QueueData> qdList = entry.getValue();

            Iterator<QueueData> it = qdList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    int perm = qd.getPerm();
                    perm &= ~PermName.PERM_WRITE;
                    qd.setPerm(perm);
                    wipeTopicCnt++;
                }
            }
        }

        return wipeTopicCnt;
    }

    /**
     * 取消注册 Broker 相关信息
     *
     * @param clusterName Broker 集群名称
     * @param brokerAddr  Broker 地址
     * @param brokerName  Broker 名称
     * @param brokerId    BrokerId
     */
    public void unregisterBroker(final String clusterName, final String brokerAddr, final String brokerName, final long brokerId) {
        try {
            try {
                // 1. 加写锁，避免更新时候被其他线程修改
                this.lock.writeLock().lockInterruptibly();

                // 2. 从存活的 Broker 列表中，移除该 Broker
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                log.info("unregisterBroker, remove from brokerLiveTable {}, {}", brokerLiveInfo != null ? "OK" : "Failed", brokerAddr);

                // 3. 从该 Broker 上注册的 Filter Server 列表中，移除该 Broker
                this.filterServerTable.remove(brokerAddr);

                // 4. 从 brokerAddrTable 列表，移除该 Broker
                // 清除 Broker 列表的标记
                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    // 根据 BrokerId ，移除该 Broker
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}", addr != null ? "OK" : "Failed", brokerAddr);

                    // 如果该 BrokerName 对应的 BrokerAdds 为空，则移除该 BrokerName 即移除 Broker 列表
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}", brokerName);

                        // 标记清除了 Broker 列表
                        removeBrokerName = true;
                    }
                }

                // 如果 BrokerName 都没有了，那么就需要处理 clusterAddrTable 集群列表，以及 topicQueueTable topic 队列列表
                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}", removed ? "OK" : "Failed", brokerName);

                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}", clusterName);
                        }
                    }
                    // todo 以 BrokerName 所有的路由信息
                    //  以 BrokerName 维度移除主题队列
                    this.removeTopicByBrokerName(brokerName);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }

    /**
     * 移除 BrokerName 所有的队列信息
     * @param brokerName
     */
    private void removeTopicByBrokerName(final String brokerName) {
        // 遍历 Topic 队列信息
        Iterator<Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, List<QueueData>> entry = itMap.next();

            String topic = entry.getKey();

            // 遍历队列集合
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                // 如果队列归属的 BrokerName 与 当前 BrokerName 相同，则移除该队列
                if (qd.getBrokerName().equals(brokerName)) {
                    log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
                    it.remove();
                }
            }

            // 如果队列集合为空，则移除该 Topic 及对应的 Topic 的队列信息
            if (queueDataList.isEmpty()) {
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
        }
    }

    /**
     * 根据 Topic 从 NameSrv 中获取 Broker 及 Topic 队列信息，组装成 Topic 路由信息。
     * 过程如下：
     * 1. 初始化返回的 TopicRouteData
     * 2. 获取操作缓存 Map 的读锁
     * 3. 根据传入的 Topic 在 topicQueueTable 中获取对应的队列信息，并写入返回结果中
     * 4. 遍历 3 中获取到的队列，获取队列所在的所有的 BrokerName，并收集起来
     * 5. 遍历 4 中收集到的 BrokerName，根据 BrokerName 从 BrokerAddrTable 中找到对应的 BrokerData ，并写入返回结果中
     * 6. 释放读锁并返回结果
     * todo 注意：根据 Topic 返回的 Broker 信息是通过队列解析处理得到的，毕竟队列信息是 Broker 上报的
     *
     * @param topic
     * @return
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {

        // 创建 topic 路由信息对象
        TopicRouteData topicRouteData = new TopicRouteData();

        // 找到 Topic 对应的队列数据
        boolean foundQueueData = false;
        // 找到 Topic 对应的 Broker 数据
        boolean foundBrokerData = false;

        Set<String> brokerNameSet = new HashSet<String>();
        List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
        topicRouteData.setBrokerDatas(brokerDataList);

        HashMap<String, List<String>> filterServerMap = new HashMap<String, List<String>>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                // 加读锁
                this.lock.readLock().lockInterruptibly();

                // 1. 先从缓冲 topicQueueTable 中获取对应的队列信息
                List<QueueData> queueDataList = this.topicQueueTable.get(topic);
                if (queueDataList != null) {
                    // 1.1 直接设置队列信息
                    topicRouteData.setQueueDatas(queueDataList);
                    foundQueueData = true;

                    // 1.2 遍历队列，取出队列对应的BrokerName，并收集起来，因为这些队列分布所在的 Broker，也是 Topic 对应的 Broker
                    Iterator<QueueData> it = queueDataList.iterator();
                    while (it.hasNext()) {
                        QueueData qd = it.next();
                        // Set 集合去重
                        brokerNameSet.add(qd.getBrokerName());
                    }

                    // 2. 遍历 brokerNameSet 集合，从 BrokerAddrTable 中获取对应的 BrokerData 数据
                    for (String brokerName : brokerNameSet) {
                        // 根据 Broker 名称找到对应的 BrokerData
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                        if (null != brokerData) {
                            // 封装 Broker 信息，此处相当于直接设置，深拷贝
                            BrokerData brokerDataClone = new BrokerData(brokerData.getCluster(), brokerData.getBrokerName(), (HashMap<Long, String>) brokerData.getBrokerAddrs().clone());

                            // todo 设置 topic 相关的 Broker 信息(是通过 Topic 对应的队列间接获取的)
                            brokerDataList.add(brokerDataClone);

                            // 设置 Broker 对应的消息过滤地址
                            foundBrokerData = true;
                            for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                                filterServerMap.put(brokerAddr, filterServerList);
                            }
                        }
                    }
                }
            } finally {
                // 释放读锁
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);

        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    /**
     * 扫描并剔除长时间未与 NameSrv 发送心跳包的 Broker
     */
    public void scanNotActiveBroker() {
        // 获取活跃 Broker 列表：brokerLiveTable
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        // 遍历 Broker 列表：brokerLiveTable
        while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();
            // Broker 最后一次发送心跳包的时间戳
            long last = next.getValue().getLastUpdateTimestamp();
            // 判断 Broker 最后一次发送心跳包的时间戳 + 120秒是否小于当前系统时间
            if ((last + BROKER_CHANNEL_EXPIRED_TIME) < System.currentTimeMillis()) {
                // 关闭与这个 Broker 的连接
                RemotingUtil.closeChannel(next.getValue().getChannel());
                // 删除这个 Broker
                //  todo 这样的话，后续 Broker 上报信息时，就可以重新注册 Broker 信息及路由信息
                it.remove();
                log.warn("The broker channel expired, {} {}ms", next.getKey(), BROKER_CHANNEL_EXPIRED_TIME);
                // 销毁这个 Broker 的网络连接
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
            }
        }
    }

    /**
     * 在 NameSrv 与 Broker 连接存在问题的时候，调用该方法
     *
     * @param remoteAddr 连接当前 NameSrv 的 Broker 地址
     * @param channel 远端 Broker 连接当前 NameSrv 的通道
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddrFound = null;
        if (channel != null) {
            try {
                try {
                    // 加读锁
                    this.lock.readLock().lockInterruptibly();

                    // 根据连接通道获取 存在问题的 broker 地址
                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable = this.brokerLiveTable.entrySet().iterator();
                    while (itBrokerLiveTable.hasNext()) {
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();
                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }
                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        // 根据 Channel 没有找到对应的 BrokerAddr 的时候，
        // 直接将传入的 BrokerAddr 作为 brokerAddrFound
        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {

            try {
                try {
                    // 加写锁
                    this.lock.writeLock().lockInterruptibly();

                    // 删除 brokerLiveTable 中的 Broker
                    this.brokerLiveTable.remove(brokerAddrFound);

                    // 删除 Broker 对应的服务过滤器
                    this.filterServerTable.remove(brokerAddrFound);

                    String brokerNameFound = null;
                    boolean removeBrokerName = false;

                    // 遍历 BrokerAddrTable
                    Iterator<Entry<String, BrokerData>> itBrokerAddrTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {

                        // 获取 BrokerName 对应的 BrokerData
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();

                        // 遍历 BrokerData 的 BrokerAddrs
                        Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> entry = it.next();
                            Long brokerId = entry.getKey();
                            String brokerAddr = entry.getValue();
                            // 匹配上 BrokerAddr 的时候，
                            // 删除 BrokerAddrs 中的 BrokerAddr，获取到 Broker 对应的 BrokerName
                            // 跳出循环
                            if (brokerAddr.equals(brokerAddrFound)) {
                                brokerNameFound = brokerData.getBrokerName();
                                it.remove();
                                log.info("remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed", brokerId, brokerAddr);
                                break;
                            }
                        }

                        // 如果 BrokerAddrs 为空，则删除 brokerAddrTable 中的 BrokerName
                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed", brokerData.getBrokerName());
                        }
                    }

                    // 删除集群中的 Broker
                    if (brokerNameFound != null && removeBrokerName) {
                        Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<String, Set<String>> entry = it.next();
                            String clusterName = entry.getKey();
                            Set<String> brokerNames = entry.getValue();
                            boolean removed = brokerNames.remove(brokerNameFound);
                            if (removed) {
                                log.info("remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed", brokerNameFound, clusterName);

                                if (brokerNames.isEmpty()) {
                                    log.info("remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster", clusterName);
                                    it.remove();
                                }

                                break;
                            }
                        }
                    }

                    // 删除在有问题的 Broker 上分布的 Topic 的队列信息
                    if (removeBrokerName) {
                        Iterator<Entry<String, List<QueueData>>> itTopicQueueTable = this.topicQueueTable.entrySet().iterator();
                        while (itTopicQueueTable.hasNext()) {
                            Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                            String topic = entry.getKey();
                            List<QueueData> queueDataList = entry.getValue();

                            Iterator<QueueData> itQueueData = queueDataList.iterator();
                            while (itQueueData.hasNext()) {
                                QueueData queueData = itQueueData.next();
                                if (queueData.getBrokerName().equals(brokerNameFound)) {
                                    itQueueData.remove();
                                    log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed", topic, queueData);
                                }
                            }

                            if (queueDataList.isEmpty()) {
                                itTopicQueueTable.remove();
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed", topic);
                            }
                        }
                    }
                } finally {
                    // 释放 写锁
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
    }

    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, List<QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, List<QueueData>> next = it.next();
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerData> next = it.next();
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerLiveInfo> next = it.next();
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Set<String>> next = it.next();
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public byte[] getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (Map.Entry<String, Set<String>> entry : clusterAddrTable.entrySet()) {
                    topicList.getTopicList().add(entry.getKey());
                    topicList.getTopicList().addAll(entry.getValue());
                }

                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) {
                        BrokerData bd = brokerAddrTable.get(it.next());
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (brokerAddrs != null && !brokerAddrs.isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);
                for (String brokerName : brokerNameSet) {
                    Iterator<Entry<String, List<QueueData>>> topicTableIt = this.topicQueueTable.entrySet().iterator();
                    while (topicTableIt.hasNext()) {
                        Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                        String topic = topicEntry.getKey();
                        List<QueueData> queueDatas = topicEntry.getValue();
                        for (QueueData queueData : queueDatas) {
                            if (brokerName.equals(queueData.getBrokerName())) {
                                topicList.getTopicList().add(topic);
                                break;
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt = this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0 && TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSysFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt = this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0 && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSysFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt = this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0 && !TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSysFlag()) && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSysFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }
}

/**
 * Broker 存活信息
 */
class BrokerLiveInfo {

    /**
     * 最后更新时间
     */
    private long lastUpdateTimestamp;

    /**
     * 数据版本
     */
    private DataVersion dataVersion;

    /**
     * 远程连接当前 NameSrv 的通道
     */
    private Channel channel;
    private String haServerAddr;

    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel, String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }

    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }

    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }

    public String getHaServerAddr() {
        return haServerAddr;
    }

    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }

    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}
