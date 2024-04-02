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
package org.apache.rocketmq.namesrv;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.srvutil.FileWatchService;

/**
 * NameServer 模块核心控制类
 */
public class NamesrvController {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /**
     * 用于指定 NameServer 的相关配置属性
     */
    private final NamesrvConfig namesrvConfig;

    /**
     * 定时任务执行线程池，默认定时执行两个任务
     * 1. 每隔 10s 扫描一次 Broker，维护当前存货的 Broker 信息
     * 2. 每隔 10m 打印下 kvConfig 配置
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
        "NSScheduledThread"));

    /**
     * todo 读取或变更 NameServer 的配置属性，加载 NamesrvConfig 中的配置的配置文件到内存
     */
    private final KVConfigManager kvConfigManager;

    /**
     * todo NameServer 数据的载体，记录 Broker 、Topic 等信息
     */
    private final RouteInfoManager routeInfoManager;

    /**
     * 实现 ChannelEventListener 接口，在发送异常时的回调方法
     * <p>
     *     主要用于在 NameServer 与 Broker 的连接通道中，在这三种情况下，在上述数据结构中移除已经宕机的 Broker
     *     (1.通道关闭
     *     (2.通道发送异常
     *     (3.通道空闲时
     * </p>
     */
    private BrokerHousekeepingService brokerHousekeepingService;

    /**
     * 用于指定 NettyServer 的相关配置属性
     * 以下 3 个属性与网络通信有关， NameServer 与 Broker 、Producer、Consumer 之间的网络通信，基于 Netty 实现
     */
    private final NettyServerConfig nettyServerConfig;
    private RemotingServer remotingServer;
    private ExecutorService remotingExecutor;

    private Configuration configuration;
    private FileWatchService fileWatchService;

    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        this.kvConfigManager = new KVConfigManager(this);
        // NameServer 数据的载体，记录 Broker 、 Topic 等信息
        this.routeInfoManager = new RouteInfoManager();
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        this.configuration = new Configuration(
            log,
            this.namesrvConfig, this.nettyServerConfig
        );
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    public boolean initialize() {

        // 加载 KV 配置
        this.kvConfigManager.load();

        // 创建 NettyServer 网络处理对象，也就是创建 Namesrv 服务器，用于处理请求，请求的处理交给 NettyRequestProcessor
        // 这样一来，NameSrv 就可以处理来自 Broker、Producer(客户端(生产者))、Consumer(客户端(消费者)) 的请求
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
        // 初始化，线程数固定为 nettyServerConfig.getServerWorkerThreads() 的固定线程池
        this.remotingExecutor =
            Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));

        /**
         * 将 DefaultProcessor 与 remotingExecutor 关联起来(绑定到一起)
         * 1.在处理请求时，NettyRemotingAbstract 会将请求包装成任务 ，然后提交给 remotingExecutor 线程池进行处理
         * 2.处理完成后，将结果通知给请求端
         * @see org.apache.rocketmq.remoting.netty.NettyRemotingServer.NettyServerHandler 处理命令的模版方法，NameSrv 会使用 DefaultRequestProcessor 处理
         */
        this.registerProcessor();

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            // 开启定时任务，每隔 10 s 扫描一次 Broker，并剔除长时间没有和 NameSrv 通信的 Broker
            // 首次延迟 5 秒执行
            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            // 开启定时任务，每隔 10 分钟打印下 kv 配置
            // 首次延迟 1 分钟执行
            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);

        if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
            // Register a listener to reload SslContext
            try {
                fileWatchService = new FileWatchService(
                    new String[] {
                        TlsSystemConfig.tlsServerCertPath,
                        TlsSystemConfig.tlsServerKeyPath,
                        TlsSystemConfig.tlsServerTrustCertPath
                    },
                    new FileWatchService.Listener() {
                        boolean certChanged, keyChanged = false;
                        @Override
                        public void onChanged(String path) {
                            if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
                                log.info("The trust certificate changed, reload the ssl context");
                                reloadServerSslContext();
                            }
                            if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
                                certChanged = true;
                            }
                            if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
                                keyChanged = true;
                            }
                            if (certChanged && keyChanged) {
                                log.info("The certificate and private key changed, reload the ssl context");
                                certChanged = keyChanged = false;
                                reloadServerSslContext();
                            }
                        }
                        private void reloadServerSslContext() {
                            ((NettyRemotingServer) remotingServer).loadSslContext();
                        }
                    });
            } catch (Exception e) {
                log.warn("FileWatchService created error, can't load the certificate dynamically");
            }
        }

        return true;
    }

    private void registerProcessor() {
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                this.remotingExecutor);
        } else {

            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
        }
    }

    /**
     * 启动 NameSrv 服务器，启动后，才能接收请求
     * @throws Exception
     */
    public void start() throws Exception {
        // 启动 NAS 组件
        this.remotingServer.start();

        if (this.fileWatchService != null) {
            this.fileWatchService.start();
        }
    }

    /**
     * 关闭 NameSrv 服务器
     */
    public void shutdown() {
        this.remotingServer.shutdown();
        this.remotingExecutor.shutdown();
        this.scheduledExecutorService.shutdown();

        if (this.fileWatchService != null) {
            this.fileWatchService.shutdown();
        }
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
