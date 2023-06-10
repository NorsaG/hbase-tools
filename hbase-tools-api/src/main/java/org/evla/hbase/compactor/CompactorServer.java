package org.evla.hbase.compactor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.jmx.JMXRegionServerMetrics;
import org.evla.hbase.meta.MetaTableHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CompactorServer implements Callable<Boolean>, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactorServer.class);

    private List<HRegionInfo> regionInfos;

    private ServerName sn;
    private final String server;
    private final ClusterStatusManageable manager;

    private final AtomicBoolean firstRun = new AtomicBoolean(true);
    private final Admin admin;
    private final HBaseToolsSettings settings;
    private final int jmxPort;

    private final boolean isInfinite;
    private final MetaTableHolder metaTableHolder;

    private final AtomicInteger allRegionsToCompact = new AtomicInteger(0);
    private final AtomicInteger doneRegions = new AtomicInteger(0);
    private JMXRegionServerMetrics jmx;

    private final LoadingCache<HRegionInfo, Object> tempCache = CacheBuilder
            .newBuilder()
            .maximumSize(350)
            .expireAfterWrite(1, TimeUnit.DAYS)
            .build(new CacheLoader<HRegionInfo, Object>() {
                @Override
                public Object load(HRegionInfo key) throws Exception {
                    return null;
                }
            });

    private static final Object object = new Object();

    private final ThreadPoolExecutor pool;
    private Map<String, RegionMetrics> loads;

    private final Map<String, CompactionWeight> statisticMap = new HashMap<>();
    private final AtomicInteger recalcCount = new AtomicInteger(-1);
    private final AtomicInteger totalCompactedVolume = new AtomicInteger(0);

    private CompactorServer(ClusterStatusManageable manager, MetaTableHolder metaTableHolder, ServerName sn, Admin admin, HBaseToolsSettings settings, int jmxPort, boolean isInfinite) {
        this.manager = manager;
        this.metaTableHolder = metaTableHolder;
        this.sn = sn;
        this.server = sn.getHostAndPort();
        this.admin = admin;
        this.settings = settings;
        this.jmxPort = jmxPort;
        this.isInfinite = isInfinite;
        this.pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(settings.getCompactorSettings().getParallelCompaction(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat(sn.getHostAndPort() + "-thread-%d").build());
    }

    CompactorServer(ClusterStatusManageable manager, MetaTableHolder metaTableHolder, List<HRegionInfo> regionInfos, ServerName sn, Admin admin, HBaseToolsSettings settings, int jmxPort) {
        this(manager, metaTableHolder, sn, admin, settings, jmxPort, false);
        this.regionInfos = regionInfos;
        this.loads = getRegionsLoad();
    }

    CompactorServer(ClusterStatusManageable manager, MetaTableHolder metaTableHolder, ServerName sn, Admin admin, HBaseToolsSettings settings, int jmxPort) {
        this(manager, metaTableHolder, sn, admin, settings, jmxPort, true);
    }

    private Map<String, RegionMetrics> getRegionsLoad() {
        try {
            ServerMetrics serverMetrics = manager.getClusterStatus().getLiveServerMetrics().get(sn);
            if (serverMetrics == null) {
                return Collections.emptyMap();
            }

            Map<String, RegionMetrics> regionMetrics = new HashMap<>();
            serverMetrics.getRegionMetrics().forEach((k, v) -> {
                String encodedRegionName = RegionInfo.getRegionNameAsString(v.getRegionName());
                regionMetrics.put(encodedRegionName, v);
            });
            return regionMetrics;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException("Cant create CompactorManager");
        }
    }

    private List<HRegionInfo> getAllRegions() {
        return new ArrayList<>(this.metaTableHolder.getAllRegionsByServer(admin.getConnection(), sn, true));
    }

    private void renameCurrentThread() {
        String name = Thread.currentThread().getName();
        Thread.currentThread().setName(name + ":" + sn.getHostAndPort());
    }

    @Override
    public Boolean call() {
        renameCurrentThread();
        // менять jmx нам не нужно, так как он привязано только к хосту
        jmx = new JMXRegionServerMetrics(sn, jmxPort);
        return isInfinite
                ? runInfiniteCompactionTasks()
                : runCompactionTasks();
    }

    @SuppressWarnings("InfiniteLoopStatement")
    private boolean runInfiniteCompactionTasks() {
        Queue<CompactionTask> compactionTasks;
        try {
            compactionTasks = getCompactionTasks(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        CompactionTask task;
        while (true) {
            try {
                if (doneRegions.get() >= settings.getCompactorSettings().getRecalculateRegionCount()) {
                    compactionTasks = getCompactionTasks(true);
                    LOGGER.info("Refresh compaction queue. New size is {}", compactionTasks.size());
                }

                if ((task = compactionTasks.poll()) != null) {
                    createAndSubmitFuture(task);
                } else {
                    LOGGER.info("There is no any regions for compaction. Waiting...");
                    sleep(settings.getCompactorSettings().getActualizeTimeout());
                    compactionTasks = getCompactionTasks(true);
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    private boolean runCompactionTasks() {
        boolean result = true;
        Queue<CompactionTask> compactionTasks = getCompactionTasks(false);
        List<Future<Boolean>> tasks = new ArrayList<>();
        try {
            int size = compactionTasks.size();
            for (CompactionTask t : compactionTasks) {
                tasks.add(createAndSubmitFuture(t));
            }
            for (Future<Boolean> f : tasks) {
                result = f.get();
            }
            LOGGER.info("Queue with {} regions was fully compacted on server {}", size, sn);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            result = false;
        } finally {
            close();
        }
        return result;
    }

    private Queue<CompactionTask> getCompactionTasks(boolean isInfinite) {
        LinkedList<CompactionTask> compactionTasks = new LinkedList<>();
        recalcCount.incrementAndGet();
        totalCompactedVolume.set(0);

        if (isInfinite) {
            doneRegions.set(0);
            if (!firstRun.get() && !validateServer()) {
                allRegionsToCompact.set(0);
                return compactionTasks;
            }
            this.regionInfos = getAllRegions();
            this.loads = getRegionsLoad();
        }

        regionInfos.forEach(ri -> {
            String encodedRegionName = RegionInfo.getRegionNameAsString(ri.getRegionName());
            RegionMetrics rl = loads.get(encodedRegionName);
            if (rl == null) {
                LOGGER.debug("Empty RegionLoad. This is valid for moved region ({})", ri);
            } else {
                CompactionWeight weight = new CompactionWeight(rl);
                if (!isInfinite) {
                    compactionTasks.add(new CompactionTask(sn, admin, ri, settings.getCompactorSettings(), weight, doneRegions, totalCompactedVolume));
                } else if (filterWeight(weight) && !tempCache.asMap().containsKey(ri)) {
                    compactionTasks.add(new CompactionTask(sn, admin, ri, settings.getCompactorSettings(), weight, doneRegions, totalCompactedVolume));
                }
                statisticMap.put(sn.getHostname() + ": " + rl.getNameAsString(), weight);
            }
        });

        if (isInfinite || settings.getCompactorSettings().isNeedSorting()) {
            allRegionsToCompact.set(compactionTasks.size());
            compactionTasks.sort(CompactionTask::compareTo);
            Collections.reverse(compactionTasks);
        }
        firstRun.set(false);
        return compactionTasks;
    }

    private boolean validateServer() {
        try {
            Collection<ServerName> servers = manager.getClusterStatus().getServers();
            if (servers.stream().anyMatch(currentServer -> sn.equals(currentServer))) {
                return true;
            }
            for (ServerName s : servers) {
                if (s.getHostAndPort().equals(server)) {
                    sn = s;
                    LOGGER.error("RegionServer restarted. New RS: {}", sn);
                    return true;
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.error("There is no active RegionServer (on {}) right now...", server);

        return false;
    }

    private boolean filterWeight(CompactionWeight weight) {
        return weight.calculateRegionCompactionWeight() > settings.getCompactorSettings().getApproximateBorderWeight() && weight.getTotalRegionSize() > settings.getCompactorSettings().getStoreSizeMb();
    }

    private Future<Boolean> createAndSubmitFuture(CompactionTask task) {
        Objects.requireNonNull(task, "task is null");
        while (true) {
            int compQueue = jmx.getCompactionQueueLength();
            int flushQueue = jmx.getFlushQueueLength();
            if (compQueue > settings.getCompactorSettings().getMaxCompactionsBorder()) {
                LOGGER.info("[{}] Compaction queue is too much right now: {}", sn, compQueue);
                sleep(10 * settings.getCompactorSettings().getAdditionDelay());
            } else if (flushQueue > settings.getCompactorSettings().getMaxFlushesBorder()) {
                LOGGER.info("[{}] Flush queue is too much right now: {}", sn, flushQueue);
                sleep(10 * settings.getCompactorSettings().getAdditionDelay());
            } else {
                if (pool.getActiveCount() < pool.getCorePoolSize()) {
                    Future<Boolean> f = pool.submit(task);
                    tempCache.put(task.getInfo(), object);
                    sleep(settings.getCompactorSettings().getAdditionDelay());
                    return f;
                } else {
                    LOGGER.debug("Compaction pool for {} is full... Waiting...", sn);
                    sleep(settings.getCompactorSettings().getAdditionDelay());
                }
            }
        }
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.shutdown();
        }
        if (jmx != null) {
            jmx.close();
        }
    }

    private static void sleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    String getInstanceName() {
        return sn.getHostname() + ":" + jmxPort;
    }

    String getStatusString() {
        if (allRegionsToCompact.get() == 0) {
            return "no regions for compaction.";
        }
        return String.format("%.2f%% (%3d of %3d)", ((double) doneRegions.get() / allRegionsToCompact.get()) * 100, doneRegions.get(), allRegionsToCompact.get());
    }

    String getStatisticString() {
        try {
            Map.Entry<String, CompactionWeight> max = statisticMap.entrySet().stream().max(Comparator.comparing(e -> e.getValue().calculateRegionCompactionWeight())).orElse(null);
            if (max != null && statisticMap.values().size() > 0) {
                List<Float> weights = statisticMap.values().stream().filter(w -> w.calculateRegionCompactionWeight() > 0).map(CompactionWeight::calculateRegionCompactionWeight).sorted().collect(Collectors.toList());
                if (weights.size() > 0) {
                    Double avg = weights.stream().collect(Collectors.averagingDouble(Float::doubleValue));
                    Float median = weights.size() % 2 == 0
                            ? (weights.get(weights.size() / 2) + weights.get(weights.size() / 2 - 1)) / 2
                            : weights.get(weights.size() / 2);
                    return String.format("%s region with max weight: %.2f; average region weight %.2f; median region weight %.2f.", max.getKey(), max.getValue().calculateRegionCompactionWeight(), avg, median);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Some error on server " + getInstanceName() + " : " + e.getMessage(), e);
        }
        return "Not calculated";
    }

    int getDoneRegionsCount() {
        return doneRegions.get();
    }

    int getAllRegionsCount() {
        return allRegionsToCompact.get();
    }

    int getRecalcCount() {
        return recalcCount.get();
    }

    int getTotalCompactedVolume() {
        return totalCompactedVolume.get();
    }
}
