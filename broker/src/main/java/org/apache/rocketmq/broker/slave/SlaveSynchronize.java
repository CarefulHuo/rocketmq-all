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
package org.apache.rocketmq.broker.slave;

import java.io.IOException;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.body.ConsumerOffsetSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

/**
 * 从主服务器同步核心数据信息
 * <p>
 *     场景分析：
 *     消息消费者首先都是从主服务拉取消息，并向其提交消息消费进度，如果当主服务器宕机后，从服务器会接管消息拉取服务，此时消息消费进度会存储在从服务器，主从服务器的消费进度会存在不一致？
 *     而且，当主服务器恢复正常之后，两者之间的消息消费进度如何同步？
 *     解答：
 *     如果 Broker 角色为从服务器，会通过定时任务调用 SlaveSynchronize#syncAll()，从主服务器定时以覆盖式的方式同步 Topic 路由信息、消息消费进度、延迟队列处理进度、消费者组订阅信息
 *     进一步问题：
 *     如果主服务器启动后，从服务器马上从主服务器同步消息进度，那不就又要重新消费消息吗？
 *     解答：
 *     其实，绝大多数情况下，就算从服务器器从主服务器同步了很久之前的消费进度，只要消费者没有重新启动，消息就不会重新被消费，在这种情况下，RocketMQ 提供了两种机制来确保消息消费进度不丢失
 *     第一种：消息消费者在内存中存储着最新的消息消费进度，继续以该进度去服务器拉取消息后，消息处理完成之后，会定时向 Broker 服务器反馈消息消费进度，而且在反馈消息消费进度时，会优先选择主服务器，此时主服务器(上线)的消息消费进度就立马被更新了，从服务器只需要定时同步主服务器的消息消费进度即可
 *     第二种：消息消费者在向服务器拉取消息的时候，如果是主服务器，在处理消息拉取时，也会更新消息消费进度，详见 @see PullMessageProcessor#processRequest
 * </p>
 * -----------------------------------------------
 * 主从同步引起的读写分离：
 * 1. 主、从服务器都在运行过程中，消息消费者拉取是从主服务器拉取还是从从服务器拉取？
 * 解答：默认情况下，RocketMQ消息消费者从主服务器上拉取消息，当主服务器积压的消息超过物理内存的 40%，则建议从从服务器上拉取，但如果从服务器的 salveReadEnable 为 false ，表示从服务器不可读，则从服务器是不会接管消息拉取的
 * 2. 当消息消费者从从服务器拉取消息后，会一直从从服务器拉取消息吗？
 * 解答：不是的，分以下情况：{@link org.apache.rocketmq.store.DefaultMessageStore#getMessage(java.lang.String, java.lang.String, int, long, int, org.apache.rocketmq.store.MessageFilter) }
 * 1）如果从服务器的 salveReadEnable 为 false ，则下次拉取，建议从主服务器拉取
 * 2）如果从服务器的 salveReadEnable 为 true && 从服务器积压的消息没有超过物理内存的 40%，则下次拉取是从消费者订阅组的 BrokerId 指定的服务器拉取的，该值默认为 0，代表主服务器
 * 3）如果从服务器的 salveReadEnable 为 true && 从服务器积压的消息超过物理内存的 40% ，则下次拉取是从消费者订阅组的 whichBrokerWhenConsumeSlowly 指定的服务器拉取，该值默认为 1，代表从服务器
 * 读写分离的正确做法：
 * 1）从服务器的 salveReadEnable 设置为 true
 * 2）通过 updateSubGroup 命令更新消息组 whichBrokerWhenConsumeSlowly、brokerId，特别是其 brokerId(从服务器) 不要设置 为 0，不然从从服务器拉取一次后，下一次拉取就会从主服务器拉取。
 * {@link PullMessageProcessor#processRequest(io.netty.channel.Channel, org.apache.rocketmq.remoting.protocol.RemotingCommand, boolean)}
 */
public class SlaveSynchronize {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    /**
     * 主服务器地址
     */
    private volatile String masterAddr = null;

    /**
     * @see BrokerController#BrokerController(org.apache.rocketmq.common.BrokerConfig, org.apache.rocketmq.remoting.netty.NettyServerConfig, org.apache.rocketmq.remoting.netty.NettyClientConfig, org.apache.rocketmq.store.config.MessageStoreConfig)
     * @param brokerController
     */
    public SlaveSynchronize(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    public String getMasterAddr() {
        return masterAddr;
    }

    public void setMasterAddr(String masterAddr) {
        this.masterAddr = masterAddr;
    }

    /**
     * 从主服务器同步，每隔 10s 执行一次
     */
    public void syncAll() {
        // 从主服务器同步 Topic 路由信息--覆盖式更新
        this.syncTopicConfig();
        // 从主服务器同步消息消费进度--覆盖式更新
        this.syncConsumerOffset();
        // 从主服务器同步延迟队列消费进度--覆盖式更新
        this.syncDelayOffset();
        // 从主服务器同步消费者组订阅信息--覆盖式更新
        this.syncSubscriptionGroupConfig();
    }

    /**
     * 从主服务器同步 Topic 路由信息
     */
    private void syncTopicConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                TopicConfigSerializeWrapper topicWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllTopicConfig(masterAddrBak);
                if (!this.brokerController.getTopicConfigManager().getDataVersion()
                    .equals(topicWrapper.getDataVersion())) {

                    this.brokerController.getTopicConfigManager().getDataVersion()
                        .assignNewOne(topicWrapper.getDataVersion());
                    this.brokerController.getTopicConfigManager().getTopicConfigTable().clear();
                    // Broker 保存全量的 Topic 信息
                    this.brokerController.getTopicConfigManager().getTopicConfigTable()
                        .putAll(topicWrapper.getTopicConfigTable());
                    // Broker 持久化 Topic 信息到本地
                    this.brokerController.getTopicConfigManager().persist();

                    log.info("Update slave topic config from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncTopicConfig Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 从主服务器同步消费进度
     */
    private void syncConsumerOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                ConsumerOffsetSerializeWrapper offsetWrapper =
                    this.brokerController.getBrokerOuterAPI().getAllConsumerOffset(masterAddrBak);

                // 保存消费组下--主题下每个消费队列的消费进度
                this.brokerController.getConsumerOffsetManager().getOffsetTable()
                    .putAll(offsetWrapper.getOffsetTable());
                // 持久化消费进度到本地
                this.brokerController.getConsumerOffsetManager().persist();
                log.info("Update slave consumer offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncConsumerOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 从主服务器同步延迟队列的消费进度
     */
    private void syncDelayOffset() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                String delayOffset =
                    this.brokerController.getBrokerOuterAPI().getAllDelayOffset(masterAddrBak);
                if (delayOffset != null) {

                    // 获取延长队列本地文件存储地址，含文件名
                    String fileName =
                        StorePathConfigHelper.getDelayOffsetStorePath(this.brokerController
                            .getMessageStoreConfig().getStorePathRootDir());
                    try {
                        // 将延迟队列消费进度持久化到本地文件
                        MixAll.string2File(delayOffset, fileName);
                    } catch (IOException e) {
                        log.error("Persist file Exception, {}", fileName, e);
                    }
                }
                log.info("Update slave delay offset from master, {}", masterAddrBak);
            } catch (Exception e) {
                log.error("SyncDelayOffset Exception, {}", masterAddrBak, e);
            }
        }
    }

    /**
     * 从主服务器同步消费组订阅信息
     */
    private void syncSubscriptionGroupConfig() {
        String masterAddrBak = this.masterAddr;
        if (masterAddrBak != null  && !masterAddrBak.equals(brokerController.getBrokerAddr())) {
            try {
                // 获取主服务器的 SubscriptionGroupWrapper
                SubscriptionGroupWrapper subscriptionWrapper =
                    this.brokerController.getBrokerOuterAPI()
                        .getAllSubscriptionGroupConfig(masterAddrBak);

                if (!this.brokerController.getSubscriptionGroupManager().getDataVersion()
                        .equals(subscriptionWrapper.getDataVersion())) {

                    SubscriptionGroupManager subscriptionGroupManager = this.brokerController.getSubscriptionGroupManager();

                    subscriptionGroupManager.getDataVersion().assignNewOne(subscriptionWrapper.getDataVersion());

                    subscriptionGroupManager.getSubscriptionGroupTable().clear();

                    // 保存消费者组订阅信息
                    subscriptionGroupManager.getSubscriptionGroupTable().putAll(subscriptionWrapper.getSubscriptionGroupTable());
                    // 持久化消费者组订阅信息到本地
                    subscriptionGroupManager.persist();
                    log.info("Update slave Subscription Group from master, {}", masterAddrBak);
                }
            } catch (Exception e) {
                log.error("SyncSubscriptionGroup Exception, {}", masterAddrBak, e);
            }
        }
    }
}
