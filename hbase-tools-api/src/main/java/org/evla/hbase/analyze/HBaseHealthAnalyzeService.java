package org.evla.hbase.analyze;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.evla.hbase.Pair;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.common.HBaseRegionChecker;
import org.evla.hbase.meta.MetaTableHolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class HBaseHealthAnalyzeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseHealthAnalyzeService.class);
    private static final SecureRandom random = new SecureRandom();
    private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final Admin admin;
    private final HBaseToolsSettings settings;
    private final MetaTableHolder holder;
    private final boolean updateMeta;

    private final AtomicInteger counter = new AtomicInteger(0);

    private Collection<ServerName> servers;
    private ScheduledExecutorService scheduler;
    private final ClusterAvailabilityObject clusterAvailabilityObject;
    private final SynchronousQueue<ClusterAvailabilityObject> queue;

    public HBaseHealthAnalyzeService(Admin admin, HBaseToolsSettings settings, MetaTableHolder holder, boolean updateMeta) {
        this.admin = admin;
        this.settings = settings;
        this.holder = holder;
        this.updateMeta = updateMeta;
        this.clusterAvailabilityObject = new ClusterAvailabilityObject(settings.getCheckerSettings().getCheckTablesCount(), 1);
        this.queue = new SynchronousQueue<>();
    }

    public ClusterAvailabilityObject checkCluster() {
        doCheck();
        return getClusterAvailabilityObject();
    }

    public void startChecking() {
        if (this.scheduler != null) {
            stopChecking();
        }

        this.scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("hbase-health-checker").build());
        this.scheduler.scheduleWithFixedDelay(this::doCheck, settings.getCheckerSettings().getCheckInterval(), settings.getCheckerSettings().getCheckInterval(), TimeUnit.SECONDS);
    }

    public void stopChecking() {
        if (this.scheduler != null) {
            this.scheduler.shutdown();
            this.scheduler = null;
        }
    }

    private Collection<ServerName> getServers() {
        try {
            return admin.getClusterMetrics().getServersName();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return Collections.emptySet();
    }

    private void doCheck() {
        if (updateMeta && (counter.get() == 0 || counter.get() % 5 == 0)) {
            LOGGER.info("Start updating meta for health checking");
            updateMetaHolder();
        }

        clusterAvailabilityObject.cleanup();

        LOGGER.info("Check writes");
        checkWrites();

        LOGGER.info("Check reads");
        checkReads();

        addResults();
        counter.incrementAndGet();
    }

    private void addResults() {
        try {
            queue.offer(clusterAvailabilityObject, settings.getCheckerSettings().getCheckInterval() / 4 * 3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error(e.getMessage(), e);
        }
    }

    public ClusterAvailabilityObject getClusterAvailabilityObject() {
        try {
           return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

    private void checkReads() {
        List<TableName> randomTables = getRandomTables();
        for (TableName tn : randomTables) {
            boolean isReadWork = readTable(tn);
            clusterAvailabilityObject.addTableResult(ClusterAvailabilityObject.Type.READ, tn, isReadWork);
        }
    }

    private void checkWrites() {
        TableName tableName = getHealthTable();
        boolean isWriteWork = mutateHealthTable(tableName);
        clusterAvailabilityObject.addTableResult(ClusterAvailabilityObject.Type.WRITE, tableName, isWriteWork);
    }

    private void updateMetaHolder() {
        LOGGER.info("Update live-servers list");
        servers = getServers();
        LOGGER.info("Read actual meta");
        holder.getAllRegions(servers, admin.getConnection());
    }

    private boolean mutateHealthTable(TableName tableName) {
        try {
            LOGGER.info("Start mutating all regions of {}", tableName);

            AtomicInteger count = new AtomicInteger(0);
            BufferedMutator mutator;

            mutator = admin.getConnection().getBufferedMutator(tableName);

            List<HRegionLocation> locations = admin.getConnection().getRegionLocator(tableName).getAllRegionLocations();
            for (HRegionLocation location : locations) {
                byte[] start = location.getRegionInfo().getStartKey();
                byte[] key = generateNextKey(start);

                Put put = new Put(key);
                put.addColumn("cf".getBytes(StandardCharsets.UTF_8),
                        "REGION_CHECK_TIME".getBytes(StandardCharsets.UTF_8),
                        LocalDateTime.now().format(DEFAULT_FORMATTER).getBytes(StandardCharsets.UTF_8));
                count.incrementAndGet();
                mutator.mutate(put);
            }

            mutator.flush();
            LOGGER.info("Successfully mutate {} records of all regions table {}", count.get(), tableName);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    private byte[] generateNextKey(byte[] start) {
        byte[] key = new byte[start.length + 4];
        byte[] salt = new byte[4];
        random.nextBytes(salt);
        System.arraycopy(start, 0, key, 0, start.length);
        System.arraycopy(salt, 0, key, start.length, salt.length);
        return key;
    }

    private TableName getHealthTable() {
        return TableName.valueOf(settings.getCheckerSettings().getHealthTable());
    }

    private List<TableName> getRandomTables() {
        try {
            List<TableName> results = new ArrayList<>();
            Map<TableName, Map<ServerName, List<String>>> tableDistributions = holder.getTablesDistribution(admin.getConnection(), servers);
            List<TableName> shuffled = new ArrayList<>(tableDistributions.keySet());

            Collections.shuffle(shuffled);

            for (TableName t : shuffled) {
                Map<ServerName, List<String>> distribution = tableDistributions.get(t);
                if (distribution == null) {
                    LOGGER.debug("Distribution of {} is unknown", t);
                    continue;
                }

                if (!servers.containsAll(distribution.keySet())) {
                    continue;
                }

                if (admin.isTableDisabled(t)) {
                    continue;
                }

                AtomicInteger regions = new AtomicInteger(0);
                distribution.forEach((k, v) -> regions.addAndGet(v.size()));

                int count = regions.get();
                if (count < 1.2 * servers.size() || count > 3 * servers.size()) {
                    continue;
                }

                results.add(t);

                if (results.size() >= settings.getCheckerSettings().getCheckTablesCount()) {
                    break;
                }
            }
            return results;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return Collections.emptyList();
        }

    }

    private boolean readTable(TableName tableName) {
        Map<RegionInfo, Pair<Result, Result>> rows;
        try {
            rows = new HBaseRegionChecker(tableName).checkTable(admin);

            int count = 1;
            StringJoiner sj = new StringJoiner("\n");
            for (Map.Entry<RegionInfo, Pair<Result, Result>> entry : rows.entrySet()) {
                StringBuilder sb = new StringBuilder();
                if (entry.getValue() == null) {
                    throw new RuntimeException("Data in table " + tableName + " is not available");
                }
                Result r1 = entry.getValue().getFirst();
                Result r2 = entry.getValue().getSecond();

                Cell c1 = null;
                if (r1 != null && r1.advance()) {
                    c1 = r1.current();
                }
                Cell c2 = null;
                if (r2 != null && r2.advance()) {
                    c2 = r2.current();
                }

                String str1 = c1 == null ? "" : Bytes.toStringBinary(CellUtil.cloneRow(c1));
                String str2 = c2 == null ? "" : Bytes.toStringBinary(CellUtil.cloneRow(c2));

                sb.append("Region #")
                        .append(count)
                        .append(" [")
                        .append(entry.getKey().getEncodedName())
                        .append("]: (");
                if (c1 != null && c2 != null) {
                    sb.append(str1).append(";").append(str2);
                } else {
                    sb.append("region is empty");
                }
                sb.append(")");

                sj.add(sb.toString());
                count++;
            }
            LOGGER.info("Table {} successfully scanned", tableName);
            LOGGER.info("\n{}", sj);

            return true;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }
}
