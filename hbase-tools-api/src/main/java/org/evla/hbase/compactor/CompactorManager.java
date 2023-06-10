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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class CompactorManager implements Compactor, ClusterStatusManageable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactorManager.class);

    private final Admin admin;
    private final HBaseToolsSettings settings;
    private ClusterStatus clusterStatus;

    private ExecutorService serversPool;
    private List<CompactorServer> compactorServersList;

    private Collection<ServerName> servers;
    private Collection<String> serverHosts;
    private final MetaTableHolder metaTableHolder;

    private final AtomicReference<Boolean> inWork;

    private final Monitoring monitor = new Monitoring("compactor");

    public CompactorManager(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.settings = settings;
        this.metaTableHolder = new MetaTableHolder();
        initService();

        this.inWork = new AtomicReference<>(true);
        LOGGER.info("{} successfully initiated.", CompactorManager.class.getSimpleName());
    }

    private void initService() {
        try {
            actualizeClusterStatus();
            this.servers = new ArrayList<>(this.clusterStatus.getServers());
            this.serverHosts = servers.stream().map(ServerName::getHostAndPort).collect(Collectors.toList());
            this.serversPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("server-compactor-%d").build());
            monitor.startMonitoring(TimeUnit.MINUTES, 1);
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
        checkInWork();
        Map<ServerName, List<HRegionInfo>> tasks = queuedRegions(regions);
        runCompactForRegions(tasks);
    }

    @Override
    public void compactTables(List<TableName> tables) {
        checkInWork();
        Map<ServerName, List<HRegionInfo>> tasks = new HashMap<>();
        try {
            for (TableName tn : tables) {
                tasks.putAll(queuedRegions(admin.getConnection().getRegionLocator(tn).getAllRegionLocations()));
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        runCompactForRegions(tasks);
    }

    @Override
    public void compactNamespaces(Set<String> namespaces) {
        checkInWork();
        Map<ServerName, List<HRegionInfo>> tasks = new HashMap<>();
        try {
            for (String namespace : namespaces) {
                for (TableName tn : admin.listTableNamesByNamespace(namespace)) {
                    tasks.putAll(queuedRegions(admin.getConnection().getRegionLocator(tn).getAllRegionLocations()));
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        runCompactForRegions(tasks);
    }

    private void runCompactForRegions(Map<ServerName, List<HRegionInfo>> tasks) {
        compactorServersList = new ArrayList<>();
        monitor.registerSingleMonitor((logger) -> {
            for (CompactorServer c : compactorServersList) {
                logger.info(c.getInstanceName() + ": " + c.getStatusString());
            }
            for (CompactorServer c : compactorServersList) {
                logger.info(c.getInstanceName() + ": " + c.getStatisticString());
            }
        });
        tasks.keySet().forEach(sn -> compactorServersList.add(new CompactorServer(this, metaTableHolder, tasks.get(sn), sn, admin, settings, settings.getCompactorSettings().getJmxPort(sn.getPort()))));
        try {
            List<Future<Boolean>> res = serversPool.invokeAll(compactorServersList);
            for (Future<Boolean> f : res) {
                if (!f.get()) {
                    throw new RuntimeException();
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void infiniteCompact() {
        checkInWork();
        compactorServersList = new ArrayList<>();
        servers.forEach(sn -> compactorServersList.add(new CompactorServer(this, metaTableHolder, sn, admin, settings, settings.getCompactorSettings().getJmxPort(sn.getPort()))));
        monitor.registerSingleMonitor((logger) -> {
            try {
                for (CompactorServer c : compactorServersList) {
                    logger.info(c.getInstanceName() + ": " + c.getStatusString());
                }
                for (CompactorServer c : compactorServersList) {
                    logger.info(c.getInstanceName() + ": " + c.getStatisticString());
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        });
        compactorServersList.forEach(serversPool::submit);
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("rs-monitor").build())
                .scheduleWithFixedDelay(this::monitorServersThreads, settings.getCompactorSettings().getRefreshDelay(), settings.getCompactorSettings().getRefreshDelay(), TimeUnit.MILLISECONDS);
    }

    private void monitorServersThreads() {
        try {
            actualizeClusterStatus();

            this.clusterStatus.getServers().forEach(rs -> {
                if (!serverHosts.contains(rs.getHostAndPort())) {
                    LOGGER.warn("There is new RegionServer in cluster. Add compactor process for {}", rs);
                    serverHosts.add(rs.getHostAndPort());
                    servers.add(rs);
                    CompactorServer cs = new CompactorServer(this, metaTableHolder, rs, admin, settings, settings.getCompactorSettings().getJmxPort(rs.getPort()));
                    compactorServersList.add(cs);
                    serversPool.submit(cs);
                }
            });
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void actualizeClusterStatus() throws IOException {
        ClusterStatus cs = admin.getClusterStatus();
        synchronized (this) {
            this.clusterStatus = cs;
        }
    }

    private Map<ServerName, List<HRegionInfo>> queuedRegions(List<HRegionLocation> regions) {
        try {
            Map<ServerName, List<HRegionInfo>> tasks = new HashMap<>();
            for (HRegionLocation loc : regions) {
                tasks.computeIfAbsent(loc.getServerName(), (s) -> new LinkedList<>()).add(loc.getRegionInfo());
            }
            return tasks;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return Collections.emptyMap();
    }

    @Override
    public void close() {
        monitor.close();
        if (!inWork.get()) {
            throw new RuntimeException("CompactorManager already closed");
        }
        this.inWork.set(false);
        LOGGER.info("Start closing all compactor resources.");
        if (compactorServersList != null && !compactorServersList.isEmpty()) {
            for (CompactorServer c : compactorServersList) {
                c.close();
            }
        }
        if (serversPool != null) {
            try {
                serversPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error("Cant stop servers pool executor", e);
            }
        }
    }

    List<CompactorServer> getCompactorServersList() {
        return compactorServersList;
    }

    @Override
    public ClusterStatus getClusterStatus() {
        return clusterStatus;
    }

}
