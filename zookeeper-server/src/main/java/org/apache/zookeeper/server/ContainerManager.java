/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 删除内存目录树中的container、ttl节点
 *
 * Manages cleanup of container ZNodes. This class is meant to only
 * be run from the leader. There's no harm in running from followers/observers
 * but that will be extra work that's not needed. Once started, it periodically
 * checks container nodes that have a cversion &gt; 0 and have no children. A
 * delete is attempted on the node. The result of the delete is unimportant.
 * If the proposal fails or the container node is not empty there's no harm.
 *
 */
public class ContainerManager {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerManager.class);

    /**
     * 内存数据库
     */
    private final ZKDatabase zkDb;

    /**
     * 请求处理器
     * 值为：zkServer.firstProcessor
     */
    private final RequestProcessor requestProcessor;

    /**
     * 检查间隔，默认60s
     * 值为：Integer.getInteger("znode.container.checkIntervalMs", (int) TimeUnit.MINUTES.toMillis(1))
     */
    private final int checkIntervalMs;

    /**
     * 每分钟删除container节点的最大个数
     * 值为："znode.container.maxPerMinute", 10000
     */
    private final int maxPerMinute;

    /**
     * container最大未使用时间
     * 值为："znode.container.maxNeverUsedIntervalMs", 0
     */
    private final long maxNeverUsedIntervalMs;

    /**
     * 定时任务
     */
    private final Timer timer;

    /**
     * 定时任务原子持有类
     */
    private final AtomicReference<TimerTask> task = new AtomicReference<TimerTask>(null);

    /**
     * @param zkDb the ZK database
     * @param requestProcessor request processer - used to inject delete
     *                         container requests
     * @param checkIntervalMs how often to check containers in milliseconds
     * @param maxPerMinute the max containers to delete per second - avoids
     *                     herding of container deletions
     */
    public ContainerManager(ZKDatabase zkDb, RequestProcessor requestProcessor, int checkIntervalMs, int maxPerMinute) {
        this(zkDb, requestProcessor, checkIntervalMs, maxPerMinute, 0);
    }

    /**
     * 初始化{@link ZooKeeperServerMain#runFromConfig(ServerConfig)}
     *
     * @param zkDb the ZK database
     * @param requestProcessor request processer - used to inject delete
     *                         container requests
     * @param checkIntervalMs how often to check containers in milliseconds
     * @param maxPerMinute the max containers to delete per second - avoids
     *                     herding of container deletions
     * @param maxNeverUsedIntervalMs the max time in milliseconds that a container that has never had
     *                                  any children is retained
     */
    public ContainerManager(ZKDatabase zkDb, RequestProcessor requestProcessor, int checkIntervalMs, int maxPerMinute, long maxNeverUsedIntervalMs) {
        this.zkDb = zkDb;
        this.requestProcessor = requestProcessor;
        this.checkIntervalMs = checkIntervalMs;
        this.maxPerMinute = maxPerMinute;
        this.maxNeverUsedIntervalMs = maxNeverUsedIntervalMs;
        timer = new Timer("ContainerManagerTask", true);

        LOG.info("Using checkIntervalMs={} maxPerMinute={} maxNeverUsedIntervalMs={}", checkIntervalMs, maxPerMinute, maxNeverUsedIntervalMs);
    }

    /**
     * 启动服务，默认每隔60s中检查一次
     *
     * start/restart the timer the runs the check. Can safely be called
     * multiple times.
     */
    public void start() {
        if (task.get() == null) {
            TimerTask timerTask = new TimerTask() {
                @Override
                public void run() {
                    try {
                        checkContainers();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        LOG.info("interrupted");
                        cancel();
                    } catch (Throwable e) {
                        LOG.error("Error checking containers", e);
                    }
                }
            };
            // 基于CAS原子操作，保证线程安全
            if (task.compareAndSet(null, timerTask)) {
                timer.scheduleAtFixedRate(timerTask, checkIntervalMs, checkIntervalMs);
            }
        }
    }

    /**
     * stop the timer if necessary. Can safely be called multiple times.
     */
    public void stop() {
        TimerTask timerTask = task.getAndSet(null);
        if (timerTask != null) {
            timerTask.cancel();
        }
        timer.cancel();
    }

    /**
     * Manually check the containers. Not normally used directly
     */
    public void checkContainers() throws InterruptedException {
        // 计算平均删除一个container节点需要的时间
        long minIntervalMs = getMinIntervalMs();

        for (String containerPath : getCandidates()) {
            long startMs = Time.currentElapsedTime();

            // 封装deleteContainer请求
            ByteBuffer path = ByteBuffer.wrap(containerPath.getBytes());
            Request request = new Request(null, 0, 0, ZooDefs.OpCode.deleteContainer, path, null);
            try {
                LOG.info("Attempting to delete candidate container: {}", containerPath);
                postDeleteRequest(request);
            } catch (Exception e) {
                LOG.error("Could not delete container: {}", containerPath, e);
            }

            long elapsedMs = Time.currentElapsedTime() - startMs;
            long waitMs = minIntervalMs - elapsedMs;

            // 删除当前container节点耗时小于预计时间，则等待，因为规定了每秒删除的阈值
            if (waitMs > 0) {
                Thread.sleep(waitMs);
            }
        }
    }

    // VisibleForTesting
    protected void postDeleteRequest(Request request) throws RequestProcessor.RequestProcessorException {
        requestProcessor.processRequest(request);
    }

    // VisibleForTesting
    protected long getMinIntervalMs() {
        return TimeUnit.MINUTES.toMillis(1) / maxPerMinute;
    }

    // VisibleForTesting
    protected Collection<String> getCandidates() {
        Set<String> candidates = new HashSet<String>();

        // 获取Container节点
        for (String containerPath : zkDb.getDataTree().getContainers()) {
            DataNode node = zkDb.getDataTree().getNode(containerPath);
            // container节点没有子节点，需要判断是否为新增节点或者开启过期功能
            if ((node != null) && node.getChildren().isEmpty()) {
                /*
                    cversion > 0: keep newly created containers from being deleted
                    before any children have been added. If you were to create the
                    container just before a container cleaning period the container
                    would be immediately be deleted.
                 */
                // 防止新创建的container被删除，因为新创建的container cversion=0
                if (node.stat.getCversion() > 0) {
                    candidates.add(containerPath);
                } else {
                    /*
                        Users may not want unused containers to live indefinitely. Allow a system
                        property to be set that sets the max time for a cversion-0 container
                        to stay before being deleted
                     */
                    // 过期节点，节点超过一定时间未被访问(无论是否新创建)。默认maxNeverUsedIntervalMs值为0
                    if ((maxNeverUsedIntervalMs != 0) && (getElapsed(node) > maxNeverUsedIntervalMs)) {
                        candidates.add(containerPath);
                    }
                }
            }

            // 上面已经包括了，重复逻辑？
            if ((node != null) && (node.stat.getCversion() > 0) && (node.getChildren().isEmpty())) {
                candidates.add(containerPath);
            }
        }

        // 获取ttl节点
        for (String ttlPath : zkDb.getDataTree().getTtls()) {
            DataNode node = zkDb.getDataTree().getNode(ttlPath);
            if (node != null) {
                Set<String> children = node.getChildren();
                // ttl节点没有子节点，才会判断是否需要清理
                if (children.isEmpty()) {
                    if (EphemeralType.get(node.stat.getEphemeralOwner()) == EphemeralType.TTL) {
                        long ttl = EphemeralType.TTL.getValue(node.stat.getEphemeralOwner());
                        // 节点距离上次被访问时间超过了ttl，需要清理
                        if ((ttl != 0) && (getElapsed(node) > ttl)) {
                            candidates.add(ttlPath);
                        }
                    }
                }
            }
        }

        return candidates;
    }

    // VisibleForTesting
    protected long getElapsed(DataNode node) {
        return Time.currentWallTime() - node.stat.getMtime();
    }

}
