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

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.common.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);

    private static final Request REQUEST_OF_DEATH = Request.requestOfDeath;

    /** The number of log entries to log before starting a snapshot */
    private static int snapCount = ZooKeeperServer.getSnapCount();

    /**
     * The total size of log entries before starting a snapshot
     */
    private static long snapSizeInBytes = ZooKeeperServer.getSnapSizeInBytes();

    /**
     * Random numbers used to vary snapshot timing
     */
    private int randRoll;
    private long randSize;

    private final BlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    private final Semaphore snapThreadMutex = new Semaphore(1);

    private final ZooKeeperServer zks;

    private final RequestProcessor nextProcessor;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    private final Queue<Request> toFlush;
    private long lastFlushTime;

    public SyncRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks.getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        this.toFlush = new ArrayDeque<>(zks.getMaxBatchSize());
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    /**
     * 计算下一次刷盘剩余时间
     * @return
     */
    private long getRemainingDelay() {
        long flushDelay = zks.getFlushDelay();
        long duration = Time.currentElapsedTime() - lastFlushTime;
        if (duration < flushDelay) {
            return flushDelay - duration;
        }
        return 0;
    }

    /** If both flushDelay and maxMaxBatchSize are set (bigger than 0), flush
     * whenever either condition is hit. If only one or the other is
     * set, flush only when the relevant condition is hit.
     */
    private boolean shouldFlush() {
        long flushDelay = zks.getFlushDelay();
        long maxBatchSize = zks.getMaxBatchSize();
        if ((flushDelay > 0) && (getRemainingDelay() == 0)) {
            return true;
        }
        return (maxBatchSize > 0) && (toFlush.size() >= maxBatchSize);
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param size
     */
    public static void setSnapSizeInBytes(long size) {
        snapSizeInBytes = size;
    }

    /**
     * 判断是否需要创建新快照,两个条件满足其一即可
     * 1. 自上次刷新快照以来,当前事务请求数大于snapCount / 2 + randRoll,randRoll是个随机值
     * 2. 自上次刷新快照以来,当前事务请求大小大于snapSizeInBytes / 2 + randSize,randSize是个随机值
     * @return
     */
    private boolean shouldSnapshot() {
        int logCount = zks.getZKDatabase().getTxnCount();
        long logSize = zks.getZKDatabase().getTxnSize();
        return (logCount > (snapCount / 2 + randRoll))
               || (snapSizeInBytes > 0 && logSize > (snapSizeInBytes / 2 + randSize));
    }

    /**
     * 重置触发新快照的随机值
     */
    private void resetSnapshotStats() {
        randRoll = ThreadLocalRandom.current().nextInt(snapCount / 2);
        randSize = Math.abs(ThreadLocalRandom.current().nextLong() % (snapSizeInBytes / 2));
    }

    @Override
    public void run() {
        try {
            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            // 刷新这两个关乎刷磁盘的参数,避免所有机器同时刷磁盘
            resetSnapshotStats();
            lastFlushTime = Time.currentElapsedTime();
            while (true) {
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_SIZE.add(queuedRequests.size());
                // 1. 计算获取新请求所需等待的时间,涉及到两个配置参数:flushDelay/maxWriteQueuePollTime
                long pollTime = Math.min(zks.getMaxWriteQueuePollTime(), getRemainingDelay());
                Request si = queuedRequests.poll(pollTime, TimeUnit.MILLISECONDS);
                // 2. 从缓存队列中获取请求为null,说明此时没有待处理请求,趁此抓紧执行刷盘操作而不需等待shouldFlush()的到来
                if (si == null) {
                    /* We timed out looking for more writes to batch, go ahead and flush immediately */
                    flush();
                    // 2.1 缓存队列中没有请求,那么当前processor阻塞在这里
                    si = queuedRequests.take();
                }
                // 3. 关闭当前processor
                if (si == REQUEST_OF_DEATH) {
                    break;
                }

                long startProcessTime = Time.currentElapsedTime();
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_TIME.add(startProcessTime - si.syncQueueStartTime);

                // track the number of records written to the log
                // 4. 将请求添加到FileTxnLog中,此时还未刷磁盘
                if (zks.getZKDatabase().append(si)) {
                    // 4.1 每成功添加一个请求到FileTxnLog就判断一下是否需要生成一个新的事务日志文件
                    if (shouldSnapshot()) {
                        resetSnapshotStats();
                        // roll the log
                        // 4.2 将当前FileTxnLog刷入磁盘,当处理下一个事务请求时,会生成新事务日志文件
                        zks.getZKDatabase().rollLog();
                        // take a snapshot
                        if (!snapThreadMutex.tryAcquire()) {
                            LOG.warn("Too busy to snap, skipping");
                        } else {
                            // 4.3 创建线程将datatree和sessions刷入FileSnap
                            new ZooKeeperThread("Snapshot Thread") {
                                public void run() {
                                    try {
                                        zks.takeSnapshot();
                                    } catch (Exception e) {
                                        LOG.warn("Unexpected exception", e);
                                    } finally {
                                        snapThreadMutex.release();
                                    }
                                }
                            }.start();
                        }
                    }
                } else if (toFlush.isEmpty()) {
                    // optimization for read heavy workloads
                    // iff this is a read, and there are no pending
                    // flushes (writes), then just pass this to the next
                    // processor
                    if (nextProcessor != null) {
                        nextProcessor.processRequest(si);
                        if (nextProcessor instanceof Flushable) {
                            ((Flushable) nextProcessor).flush();
                        }
                    }
                    continue;
                }
                toFlush.add(si);
                if (shouldFlush()) {
                    flush();
                }
                ServerMetrics.getMetrics().SYNC_PROCESS_TIME.add(Time.currentElapsedTime() - startProcessTime);
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    private void flush() throws IOException, RequestProcessorException {
        if (this.toFlush.isEmpty()) {
            return;
        }

        ServerMetrics.getMetrics().BATCH_SIZE.add(toFlush.size());

        long flushStartTime = Time.currentElapsedTime();
        zks.getZKDatabase().commit();
        ServerMetrics.getMetrics().SYNC_PROCESSOR_FLUSH_TIME.add(Time.currentElapsedTime() - flushStartTime);

        if (this.nextProcessor == null) {
            this.toFlush.clear();
        } else {
            while (!this.toFlush.isEmpty()) {
                final Request i = this.toFlush.remove();
                long latency = Time.currentElapsedTime() - i.syncQueueStartTime;
                ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUE_AND_FLUSH_TIME.add(latency);
                this.nextProcessor.processRequest(i);
            }
            if (this.nextProcessor instanceof Flushable) {
                ((Flushable) this.nextProcessor).flush();
            }
            lastFlushTime = Time.currentElapsedTime();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        queuedRequests.add(REQUEST_OF_DEATH);
        try {
            this.join();
            this.flush();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while wating for {} to finish", this);
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(final Request request) {
        Objects.requireNonNull(request, "Request cannot be null");

        request.syncQueueStartTime = Time.currentElapsedTime();
        queuedRequests.add(request);
        ServerMetrics.getMetrics().SYNC_PROCESSOR_QUEUED.add(1);
    }

}
