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

package org.apache.rocketmq.broker.filtersrv;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerStartup;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;

/**
 * 该类的作用：管理和维护消息过滤服务的生命周期，确保消息可以根据需要进行过滤。
 * 负责启动、注册、管理和关闭Filter Server（消息过滤服务器）
 */
public class FilterServerManager {

    public static final long FILTER_SERVER_MAX_IDLE_TIME_MILLS = 30000;
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    /**
     * 用于存储与 Netty 通道相关联的 FilterServerInfo 对象(它用于记录已注册的Filter Server地址和它们的最后更新时间)
     */
    private final ConcurrentMap<Channel, FilterServerInfo> filterServerTable = new ConcurrentHashMap<Channel, FilterServerInfo>(16);
    /**
     * 提供对 Broker 配置和操作的访问。
     */
    private final BrokerController brokerController;

    /**
     * 用于定期执行任务，例如创建 Filter Server
     */
    private ScheduledExecutorService scheduledExecutorService = Executors.
            newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FilterServerManagerScheduledThread"));

    /**
     * 构造函数初始化 brokerController 并创建一个单线程的计划执行服务
     *
     * @param brokerController
     */
    public FilterServerManager(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    /**
     * 启动一个定时任务，该任务以固定延迟调用createFilterServer()方法。
     */
    public void start() {

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    FilterServerManager.this.createFilterServer();
                } catch (Exception e) {
                    log.error("", e);
                }
            }
        }, 1000 * 5, 1000 * 30, TimeUnit.MILLISECONDS);
    }

    /**
     * 根据配置的Filter Server数量创建新的Filter Server实例。
     */
    public void createFilterServer() {
        int more = this.brokerController.getBrokerConfig().getFilterServerNums() - this.filterServerTable.size();
        String cmd = this.buildStartCommand();
        for (int i = 0; i < more; i++) {
            FilterServerUtil.callShell(cmd, log);
        }
    }

    /**
     * 构建启动Filter Server的命令。根据操作系统类型和配置文件路径生成相应的命令。
     *
     * @return
     */
    private String buildStartCommand() {
        String config = "";
        if (BrokerStartup.configFile != null) {
            config = String.format("-c %s", BrokerStartup.configFile);
        }

        if (this.brokerController.getBrokerConfig().getNamesrvAddr() != null) {
            config += String.format(" -n %s", this.brokerController.getBrokerConfig().getNamesrvAddr());
        }

        if (RemotingUtil.isWindowsPlatform()) {
            return String.format("start /b %s\\bin\\mqfiltersrv.exe %s", this.brokerController.getBrokerConfig().getRocketmqHome(), config);
        } else {
            return String.format("sh %s/bin/startfsrv.sh %s", this.brokerController.getBrokerConfig().getRocketmqHome(), config);
        }
    }

    /**
     * 关闭线程池
     */
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    /**
     * 注册一个新的 Filter Server，将其添加到 filterServerTable 中。
     * 如果 Filter Server 已经存在，则更新其 lastUpdateTimestamp。
     *
     * @param channel
     * @param filterServerAddr
     */
    public void registerFilterServer(final Channel channel, final String filterServerAddr) {
        FilterServerInfo filterServerInfo = this.filterServerTable.get(channel);
        if (filterServerInfo != null) {
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
        } else {
            filterServerInfo = new FilterServerInfo();
            filterServerInfo.setFilterServerAddr(filterServerAddr);
            filterServerInfo.setLastUpdateTimestamp(System.currentTimeMillis());
            this.filterServerTable.put(channel, filterServerInfo);
            log.info("Receive a New Filter Server<{}>", filterServerAddr);
        }
    }

    /**
     * 扫描并移除长时间未更新的Filter Server。
     */
    public void scanNotActiveChannel() {

        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            long timestamp = next.getValue().getLastUpdateTimestamp();
            Channel channel = next.getKey();
            // 如果当前时间与最后一次更新时间之差超过了 FILTER_SERVER_MAX_IDLE_TIME_MILLS，
            // 则认为该 Filter Server 已过期，将其从 filterServerTable 中移除并关闭对应的Channel。
            if ((System.currentTimeMillis() - timestamp) > FILTER_SERVER_MAX_IDLE_TIME_MILLS) {
                log.info("The Filter Server<{}> expired, remove it", next.getKey());
                it.remove();
                RemotingUtil.closeChannel(channel);
            }
        }
    }

    /**
     * 当 Filter Server 的通道关闭时，从 filterServerTable 中移除对应的记录。
     *
     * @param remoteAddr
     * @param channel
     */
    public void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        FilterServerInfo old = this.filterServerTable.remove(channel);
        if (old != null) {
            log.warn("The Filter Server<{}> connection<{}> closed, remove it", old.getFilterServerAddr(), remoteAddr);
        }
    }

    /**
     * 返回当前所有已注册 Filter Server 的地址列表。
     *
     * @return
     */
    public List<String> buildNewFilterServerList() {
        List<String> addr = new ArrayList<>();
        Iterator<Entry<Channel, FilterServerInfo>> it = this.filterServerTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Channel, FilterServerInfo> next = it.next();
            addr.add(next.getValue().getFilterServerAddr());
        }
        return addr;
    }

    /**
     * 该静态内部类用于存储过滤服务器的地址和最后更新时间戳。
     */
    static class FilterServerInfo {
        private String filterServerAddr;
        private long lastUpdateTimestamp;

        public String getFilterServerAddr() {
            return filterServerAddr;
        }

        public void setFilterServerAddr(String filterServerAddr) {
            this.filterServerAddr = filterServerAddr;
        }

        public long getLastUpdateTimestamp() {
            return lastUpdateTimestamp;
        }

        public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
            this.lastUpdateTimestamp = lastUpdateTimestamp;
        }
    }
}
