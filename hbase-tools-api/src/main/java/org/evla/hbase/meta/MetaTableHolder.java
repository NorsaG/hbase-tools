package org.evla.hbase.meta;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class MetaTableHolder {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaTableHolder.class);
    private final Map<ServerName, Set<HRegionInfo>> regionsByServer = new HashMap<>();

    private ExecutorService getExecutorService(int servers) {
        return Executors.newFixedThreadPool(servers, new ThreadFactoryBuilder().setNameFormat("meta-reader-%d").build());
    }

    public Set<HRegionInfo> getAllRegionsFromMemory() {
        Set<HRegionInfo> regions = new HashSet<>();
        regionsByServer.values().forEach(regions::addAll);
        return regions;
    }

    public Map<TableName, Map<ServerName, List<String>>> getTablesDistribution(Connection connection, Collection<ServerName> servers) {
        return getTablesDistribution(connection, servers, false);
    }

    public Map<TableName, Map<ServerName, List<String>>> getTablesDistribution(Connection connection, Collection<ServerName> servers, boolean needUpdate) {
        Map<TableName, Map<ServerName, List<String>>> results = new ConcurrentHashMap<>();

        if (regionsByServer.isEmpty() || needUpdate) {
            List<Callable<Void>> tasks = new ArrayList<>();
            LOGGER.debug("Get cluster distribution info from hbase:meta");
            servers.parallelStream().forEach(sn -> tasks.add(() -> {
                LOGGER.debug("Get distribution info from for {} from hbase:meta", sn);
                updateRegion(connection, sn);
                Set<HRegionInfo> regions = regionsByServer.get(sn);
                for (HRegionInfo info : regions) {
                    TableName tn = info.getTable();
                    results.putIfAbsent(tn, new ConcurrentHashMap<>());
                    results.get(tn).putIfAbsent(sn, new ArrayList<>());
                    results.get(tn).get(sn).add(info.getEncodedName());
                }
                return null;
            }));
            ExecutorService es = getExecutorService(servers.size());
            executeActions(tasks, es);

            results.forEach((tn, map) -> {
                for (ServerName sn : servers) {
                    map.putIfAbsent(sn, new ArrayList<>());
                }
            });

            es.shutdown();
        } else {
            LOGGER.info("Calculate distribution info from memory");
            servers.parallelStream().forEach(sn -> {
                LOGGER.debug("Calculate distribution info from memory for {}", sn);
                Set<HRegionInfo> regions = regionsByServer.get(sn);
                for (HRegionInfo info : regions) {
                    TableName tn = info.getTable();
                    results.putIfAbsent(tn, new ConcurrentHashMap<>());
                    results.get(tn).putIfAbsent(sn, new ArrayList<>());
                    results.get(tn).get(sn).add(info.getEncodedName());
                }
            });
        }
        return results;
    }

    public Set<HRegionInfo> getAllRegionsByServer(Connection connection, ServerName serverName) {
        if (regionsByServer.get(serverName) == null)
            updateRegion(connection, serverName);

        return regionsByServer.get(serverName);
    }

    public Set<HRegionInfo> getAllRegionsByServer(Connection connection, ServerName serverName, boolean isNeedUpdate) {
        if (isNeedUpdate) {
            updateRegion(connection, serverName);
        }
        Set<HRegionInfo> res = regionsByServer.get(serverName);
        return res == null ? Collections.emptySet() : res;
    }

    public void updateRegion(Connection connection, ServerName serverName) {
        if (connection != null) {
            LOGGER.debug("Start reading info from hbase:meta for {}", serverName);
            Set<HRegionInfo> regions = MetaTableInfoService.getTableRegions(connection, serverName);

            regionsByServer.remove(serverName);
            regionsByServer.put(serverName, regions);
        }
    }

    public Map<ServerName, Set<HRegionInfo>> getAllRegions(Collection<ServerName> servers, Connection connection) {
        LOGGER.info("Get all regions from cluster and make mapping to RegionSevers");
        if (regionsByServer.isEmpty()) {
            List<Callable<Void>> tasks = new ArrayList<>();
            servers.forEach(sn -> tasks.add(() -> {
                updateRegion(connection, sn);
                return null;
            }));
            ExecutorService es = getExecutorService(servers.size());
            executeActions(tasks, es);
            es.shutdown();
        }
        return regionsByServer;
    }

    private static void executeActions(Collection<Callable<Void>> actions, ExecutorService executorService) {
        try {
            List<Future<Void>> futures = executorService.invokeAll(actions);
            for (Future<Void> f : futures) {
                executeFuture(f);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e.getMessage(), e);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void executeFuture(Future<Void> f) throws InterruptedException {
        try {
            f.get();
            if (!f.isDone()) {
                throw new RuntimeException("Cant execute tasks");
            }
        } catch (ExecutionException e) {
            throw new RuntimeException("Error while executing: " + e.getMessage(), e);
        }
    }
}
