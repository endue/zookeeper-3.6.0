/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.audit.ZKAuditProvider;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.impl.MetricsProviderBootstrap;
import org.apache.zookeeper.server.admin.AdminServer;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.admin.AdminServerFactory;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.DatadirException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.JvmPauseMonitor;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class starts and runs a standalone ZooKeeperServer.
 */
@InterfaceAudience.Public
public class ZooKeeperServerMain {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperServerMain.class);

    private static final String USAGE = "Usage: ZooKeeperServerMain configfile | port datadir [ticktime] [maxcnxns]";

    // ZooKeeper server supports two kinds of connection: unencrypted and encrypted.
    private ServerCnxnFactory cnxnFactory;
    private ServerCnxnFactory secureCnxnFactory;
    private ContainerManager containerManager;
    private MetricsProvider metricsProvider;
    private AdminServer adminServer;

    /*
     * 单机模式初始化&启动入口
     *
     * Start up the ZooKeeper server.
     *
     * @param args the configfile or the port datadir [ticktime]
     */
    public static void main(String[] args) {
        ZooKeeperServerMain main = new ZooKeeperServerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (DatadirException e) {
            LOG.error("Unable to access datadir, exiting abnormally", e);
            System.err.println("Unable to access datadir, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNABLE_TO_ACCESS_DATADIR.getValue());
        } catch (AdminServerException e) {
            LOG.error("Unable to start AdminServer, exiting abnormally", e);
            System.err.println("Unable to start AdminServer, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.ERROR_STARTING_ADMIN_SERVER.getValue());
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        LOG.info("Exiting normally");
        ServiceUtils.requestSystemExit(ExitCode.EXECUTION_FINISHED.getValue());
    }

    /**
     * 从以下代码可以看出输入的参数有两种情况:
     * 1. 只有一个参数,按照配置文件路径解析
     * 2. 不是一个参数,按照配置参数解析,顺序:clientPortAddress，dataDir，dataLogDir，tickTime，maxClientCnxns
     * @param args
     * @throws ConfigException
     * @throws IOException
     * @throws AdminServerException
     */
    protected void initializeAndRun(String[] args) throws ConfigException, IOException, AdminServerException {
        try {
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }
        // 创建单机版配置文件类
        ServerConfig config = new ServerConfig();
        if (args.length == 1) {
            config.parse(args[0]);
        } else {
            config.parse(args);
        }

        runFromConfig(config);
    }

    /**
     * 基于zk单机模式服务配置文件启动
     * Run from a ServerConfig.
     * @param config ServerConfig to use.
     * @throws IOException
     * @throws AdminServerException
     */
    public void runFromConfig(ServerConfig config) throws IOException, AdminServerException {
        LOG.info("Starting server");
        FileTxnSnapLog txnLog = null;
        try {
            // 1. zk服务各项指标?
            try {
                metricsProvider = MetricsProviderBootstrap.startMetricsProvider(
                    config.getMetricsProviderClassName(),
                    config.getMetricsProviderConfiguration());
            } catch (MetricsProviderLifeCycleException error) {
                throw new IOException("Cannot boot MetricsProvider " + config.getMetricsProviderClassName(), error);
            }
            ServerMetrics.metricsProviderInitialized(metricsProvider);
            // Note that this thread isn't going to be doing anything else,
            // so rather than spawning another thread, we will just call
            // run() in this thread.
            // create a file logger url from the command line args
            // 2. 创建数据日志和数据目录文件夹
            txnLog = new FileTxnSnapLog(config.dataLogDir, config.dataDir);
            // 3. JVM GC监控
            JvmPauseMonitor jvmPauseMonitor = null;
            if (config.jvmPauseMonitorToRun) {
                jvmPauseMonitor = new JvmPauseMonitor(config);
            }
            // 4. 创建zk服务
            final ZooKeeperServer zkServer = new ZooKeeperServer(jvmPauseMonitor, txnLog, config.tickTime, config.minSessionTimeout, config.maxSessionTimeout, config.listenBacklog, null, config.initialConfig);
            // 5. 将zk服务统计信息对象设置到日志对象中
            txnLog.setServerStats(zkServer.serverStats());

            // Registers shutdown handler which will be used to know the
            // server error or shutdown state changes.
            // 6. 注册zk服务器关闭或异常监听器
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            zkServer.registerServerShutdownHandler(new ZooKeeperServerShutdownHandler(shutdownLatch));

            // Start Admin server
            // 7. 命令服务，参考https://zookeeper.apache.org/doc/r3.5.0-alpha/zookeeperAdmin.html#sc_adminserver，内部基于jetty服务接收get请求执行不同命令输出相关信息
            adminServer = AdminServerFactory.createAdminServer();
            adminServer.setZooKeeperServer(zkServer);
            adminServer.start();

            // 8. 启动相关服务,用户监听客户的请求
            // NIO默认不支持SSL,需要Netty模式
            boolean needStartZKServer = true;
            if (config.getClientPortAddress() != null) {
                cnxnFactory = ServerCnxnFactory.createFactory();
                cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns(), config.getClientPortListenBacklog(), false);
                cnxnFactory.startup(zkServer);
                // zkServer has been started. So we don't need to start it again in secureCnxnFactory.
                // 已启动zkServer服务。所以我们不需要在secureCnxnFactory中再次启动它
                needStartZKServer = false;
            }
            if (config.getSecureClientPortAddress() != null) {
                secureCnxnFactory = ServerCnxnFactory.createFactory();
                secureCnxnFactory.configure(config.getSecureClientPortAddress(), config.getMaxClientCnxns(), config.getClientPortListenBacklog(), true);
                secureCnxnFactory.startup(zkServer, needStartZKServer);
            }

            // 9. 启动container节点管理器
            containerManager = new ContainerManager(
                zkServer.getZKDatabase(),
                zkServer.firstProcessor,
                Integer.getInteger("znode.container.checkIntervalMs", (int) TimeUnit.MINUTES.toMillis(1)),
                Integer.getInteger("znode.container.maxPerMinute", 10000),
                Long.getLong("znode.container.maxNeverUsedIntervalMs", 0)
            );
            containerManager.start();

            // 10. 服务启动关停日志
            ZKAuditProvider.addZKStartStopAuditLog();

            // Watch status of ZooKeeper server. It will do a graceful shutdown
            // if the server is not running or hits an internal error.
            shutdownLatch.await();

            shutdown();

            if (cnxnFactory != null) {
                cnxnFactory.join();
            }
            if (secureCnxnFactory != null) {
                secureCnxnFactory.join();
            }
            if (zkServer.canShutdown()) {
                zkServer.shutdown(true);
            }
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Server interrupted", e);
        } finally {
            if (txnLog != null) {
                txnLog.close();
            }
            if (metricsProvider != null) {
                try {
                    metricsProvider.stop();
                } catch (Throwable error) {
                    LOG.warn("Error while stopping metrics", error);
                }
            }
        }
    }

    /**
     * Shutdown the serving instance
     */
    protected void shutdown() {
        if (containerManager != null) {
            containerManager.stop();
        }
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }
        if (secureCnxnFactory != null) {
            secureCnxnFactory.shutdown();
        }
        try {
            if (adminServer != null) {
                adminServer.shutdown();
            }
        } catch (AdminServerException e) {
            LOG.warn("Problem stopping AdminServer", e);
        }
    }

    // VisibleForTesting
    ServerCnxnFactory getCnxnFactory() {
        return cnxnFactory;
    }

}
