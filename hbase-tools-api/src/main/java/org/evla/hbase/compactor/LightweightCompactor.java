package org.evla.hbase.compactor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.Monitoring;
import org.evla.hbase.rstask.RSTaskControllerHelper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LightweightCompactor implements Compactor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LightweightCompactor.class);

    private final Admin admin;
    private final HBaseToolsSettings settings;

    private ExecutorService serversPool;
    private Map<ServerName, QueuedCompactorServer> compactorServersList;

    private Collection<ServerName> servers;

    private final AtomicBoolean isStopped = new AtomicBoolean(false);
    private final Monitoring monitor = new Monitoring("partial-compactor");

    public LightweightCompactor(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.settings = settings;
        initService(admin);
        this.compactorServersList = new HashMap<>();
        monitor.registerSingleMonitor((logger) -> {
            AtomicInteger done = new AtomicInteger(0);
            AtomicInteger scheduled = new AtomicInteger(0);
            AtomicInteger inProgress = new AtomicInteger(0);
            for (QueuedCompactorServer c : compactorServersList.values()) {
                done.addAndGet(c.getCompactedTasksNumber());
                scheduled.addAndGet(c.getScheduledTasksNumber());
                inProgress.addAndGet(c.getInProgressTasks());
            }
            LOGGER.info("There are {} regions for compact.{} regions compacting right now. {} regions already compacted", scheduled.get(), inProgress.get(), done.get());
        });

        infiniteCompact();
        LOGGER.info("{} successfully initiated.", CompactorManager.class.getSimpleName());
    }

    private void initService(Admin admin) {
        ClusterStatus status;
        try {
            status = admin.getClusterStatus();
            this.servers = status.getServers();
            this.serversPool = Executors.newFixedThreadPool(servers.size(), new ThreadFactoryBuilder().setNameFormat("compactor-thread-%d").build());
            monitor.startMonitoring(TimeUnit.MINUTES, 1);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void compactRegions(List<HRegionLocation> regions) {
        regions.forEach(this::addTaskForCompact);
    }

    @Override
    public void compactTables(List<TableName> tables) {
        tables.forEach(tn -> {
            try {
                compactRegions(admin.getConnection().getRegionLocator(tn).getAllRegionLocations());
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void compactNamespaces(Set<String> namespaces) {
        for (String namespace : namespaces) {
            try {
                compactTables(Arrays.asList(admin.listTableNamesByNamespace(namespace)));
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void infiniteCompact() {
        compactorServersList = new HashMap<>();
        servers.forEach(sn -> compactorServersList.put(sn, new QueuedCompactorServer(sn, admin, settings, settings.getCompactorSettings().getJmxPort(sn.getPort()))));

        compactorServersList.values().forEach(serversPool::submit);
    }

    @Override
    public void close() {
        monitor.close();
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(200));
        if (!isStopped.get()) {
            stopCompact();
            compactorServersList.values().parallelStream().forEach(QueuedCompactorServer::close);
            return;
        }

        LOGGER.info("Start closing all compactor resources.");
        if (compactorServersList != null && !compactorServersList.isEmpty()) {
            compactorServersList.values().parallelStream().forEach(QueuedCompactorServer::close);
        }
        if (serversPool != null) {
            serversPool.shutdown();
        }
        LOGGER.info("Compactor closed");
    }

    private void addTaskForCompact(HRegionLocation location) {
        QueuedCompactorServer cs = compactorServersList.get(location.getServerName());
        cs.addCompactionTask(location);
    }

    public void stopCompact() {
        while (true) {
            boolean hasTasks = false;
            for (QueuedCompactorServer s : compactorServersList.values()) {
                if (s.hasPreparedTasks()) {
                    RSTaskControllerHelper.sleep(3_000);
                    hasTasks = true;
                    break;
                }
            }

            if (!hasTasks) {
                isStopped.set(true);
                break;
            }
        }
        close();
    }
}