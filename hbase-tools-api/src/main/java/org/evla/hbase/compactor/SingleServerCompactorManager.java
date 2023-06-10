package org.evla.hbase.compactor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.Monitoring;
import org.evla.hbase.meta.MetaTableHolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class SingleServerCompactorManager implements Compactor, ClusterStatusManageable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleServerCompactorManager.class);

    private final Admin admin;
    private final HBaseToolsSettings settings;
    private ClusterStatus clusterStatus;

    private ExecutorService serverPool;
    private CompactorServer compactorServer;

    private ServerName server;
    private final MetaTableHolder metaTableHolder;

    private final AtomicReference<Boolean> inWork;
    private final Monitoring monitor = new Monitoring("single-server-compactor");

    public SingleServerCompactorManager(Admin admin, HBaseToolsSettings settings, String serverName) {
        this.admin = admin;
        this.settings = settings;
        this.metaTableHolder = new MetaTableHolder();

        initService(serverName);

        this.inWork = new AtomicReference<>(true);
        LOGGER.info("{} successfully initiated.", SingleServerCompactorManager.class.getSimpleName());
    }

    private void initService(String serverName) {
        try {
            this.clusterStatus = admin.getClusterStatus();
            ServerName tmp = ServerName.parseServerName(serverName);
            for (ServerName sn : clusterStatus.getServers()) {
                if (Objects.equals(tmp.getHostAndPort(), sn.getHostAndPort())) {
                    this.server = sn;
                    break;
                }
            }

            if (server == null) {
                throw new RuntimeException("Cant find server " + serverName);
            }
            this.serverPool = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("server-" + server + "-compactor").build());
            this.monitor.startMonitoring(TimeUnit.MINUTES, 1);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e.getMessage());
        }
    }

    private void checkInWork() {
        if (!inWork.get()) {
            throw new RuntimeException("CompactorManager is closed");
        }
    }

    @Override
    public void compactRegions(List<HRegionLocation> regions) {
        throw new IllegalArgumentException("Operation is not supported");
    }

    @Override
    public void compactTables(List<TableName> tables) {
        throw new IllegalArgumentException("Operation is not supported");
    }

    @Override
    public void compactNamespaces(Set<String> namespaces) {
        throw new IllegalArgumentException("Operation is not supported");
    }

    @Override
    public void infiniteCompact() {
        throw new IllegalArgumentException("Operation is not supported");
    }

    @Override
    public void compact() {
        List<HRegionInfo> allRegions = getTasksForServer();
        LOGGER.info("Start compacting all problem regions for {}", server);
        runCompactForRegions(allRegions);
        LOGGER.info("{} already compacted", server);
        close();
    }

    private List<HRegionInfo> getTasksForServer() {
        return new ArrayList<>(metaTableHolder.getAllRegionsByServer(admin.getConnection(), server));
    }

    private void runCompactForRegions(List<HRegionInfo> tasks) {
        monitor.registerSingleMonitor((logger) -> {
            logger.info(compactorServer.getInstanceName() + ": " + compactorServer.getStatusString());
            logger.info(compactorServer.getInstanceName() + ": " + compactorServer.getStatisticString());
        });
        compactorServer = new CompactorServer(this, metaTableHolder, tasks, server, admin, settings, settings.getCompactorSettings().getJmxPort(server.getPort()));
        try {
            Future<Boolean> res = serverPool.submit(compactorServer);
            if (!res.get()) {
                throw new RuntimeException();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        monitor.close();
        if (!inWork.get()) {
            throw new RuntimeException("CompactorManager already closed");
        }
        this.inWork.set(false);
        LOGGER.info("Start closing all compactor resources.");
        if (compactorServer != null) {
            compactorServer.close();
        }
        if (serverPool != null) {
            try {
                serverPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Cant stop servers pool executor", e);
            }
        }
    }

    @Override
    public ClusterStatus getClusterStatus() {
        return clusterStatus;
    }

}
