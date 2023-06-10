package org.evla.hbase.compactor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.rstask.RSTaskControllerHelper;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.jmx.JMXRegionServerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class QueuedCompactorServer implements Callable<Boolean>, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueuedCompactorServer.class);

    private final ServerName sn;
    private final Admin admin;
    private final HBaseToolsSettings settings;
    private final int jmxPort;

    private JMXRegionServerMetrics jmx;

    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final ThreadPoolExecutor pool;

    private final Queue<CompactionTask> tasks;

    QueuedCompactorServer(ServerName sn, Admin admin, HBaseToolsSettings settings, int jmxPort) {
        this.sn = sn;
        this.admin = admin;
        this.settings = settings;
        this.jmxPort = jmxPort;
        this.tasks = new LinkedList<>();
        this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(settings.getCompactorSettings().getParallelCompaction(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat(sn.getHostAndPort() + "-compact-thread").build());
    }

    @Override
    public Boolean call() {
        jmx = new JMXRegionServerMetrics(sn, jmxPort);
        return runCompactionTasks();
    }

    private boolean runCompactionTasks() {
        CompactionTask task;
        while (true) {
            try {
                if ((task = tasks.poll()) != null) {
                    submitFuture(task);
                } else {
                    if (!isStopped.get()) {
                        LOGGER.debug("There is no any regions for compaction. Waiting...");
                        RSTaskControllerHelper.sleep(settings.getCompactorSettings().getAdditionDelay());
                    } else {
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        return true;
    }

    private Future<Boolean> submitFuture(CompactionTask task) {
        Objects.requireNonNull(task, "task is null");
        while (true) {
            int compQueue = jmx.getCompactionQueueLength();
            int flushQueue = jmx.getFlushQueueLength();
            if (compQueue > settings.getCompactorSettings().getMaxCompactionsBorder()) {
                LOGGER.info("[{}] Compaction queue is too much right now: {}", sn, compQueue);
                RSTaskControllerHelper.sleep(10 * settings.getCompactorSettings().getAdditionDelay());
            } else if (flushQueue > settings.getCompactorSettings().getMaxFlushesBorder()) {
                LOGGER.info("[{}] Flush queue is too much right now: {}", sn, flushQueue);
                RSTaskControllerHelper.sleep(10 * settings.getCompactorSettings().getAdditionDelay());
            } else {
                if (pool.getActiveCount() < pool.getCorePoolSize()) {
                    Future<Boolean> f = pool.submit(task);
                    RSTaskControllerHelper.sleep(settings.getCompactorSettings().getAdditionDelay());
                    return f;
                } else {
                    LOGGER.debug("Compaction pool for {} is full... Waiting...", sn);
                    RSTaskControllerHelper.sleep(settings.getCompactorSettings().getAdditionDelay());
                }
            }
        }
    }

    @Override
    public void close() {
        if (!isStopped.get()) {
            stop();
        }
        if (pool != null) {
            try {
                pool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
        if (jmx != null) {
            jmx.close();
        }
    }

    public void stop() {
        isStopped.set(true);
    }

    String getInstanceName() {
        return sn.getHostname() + ":" + jmxPort;
    }

    String getStatisticString() {
        return String.format("Current queue: %3d. Already compacted: %3d", tasks.size(), counter.get());
    }

    void addCompactionTask(HRegionLocation location) {
        tasks.add(new CompactionTask(sn, admin, location.getRegionInfo(), settings.getCompactorSettings(), null, counter, null));
    }

    boolean hasPreparedTasks() {
        return tasks.size() > 0;
    }

    public int getCompactedTasksNumber() {
        return counter.get();
    }

    public int getInProgressTasks() {
        return pool.getActiveCount();
    }

    public int getScheduledTasksNumber() {
        return tasks.size();
    }
}
