package org.evla.hbase.analyze;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TableAnalyzer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableAnalyzer.class);
    private static final int MB = 1024;
    private final Admin admin;

    TableAnalyzer(Admin admin) {
        this.admin = admin;
    }

    public void run(File file) {
        try {
            LOGGER.info("Start analyze tables from: {}", file.toString());
            List<String> tables = Files.readAllLines(file.toPath());
            for (String table : tables) {
                run(TableName.valueOf(table));
            }
            LOGGER.info("Stop analyze");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void run(String mask) {
        try {
            LOGGER.info("Start analyze for tables: {}", mask);
            TableName[] tables = admin.listTableNames(mask);
            for (TableName tn : tables) {
                run(tn);
            }
            LOGGER.info("Stop analyze");
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void run(TableName tableName) {
        try {
            LOGGER.info("Start analyze for {}", tableName);
            ClusterMetrics metrics = admin.getClusterMetrics();
            Map<String, RegionMetrics> tableLoads = getTableLoad(metrics, tableName);
            LOGGER.info("There are {} regions in table (by Region Metrics)", tableLoads.size());
            StringJoiner sj = new StringJoiner("\n");

            calculateFullSize(tableLoads, sj);

            calculateSize(tableLoads, sj);
            calculateDistribution(tableName, sj);
            calculateLocality(tableLoads, sj);
            calculateFileCounts(tableLoads, sj);

            LOGGER.info("\n" + sj);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void calculateFullSize(Map<String, RegionMetrics> tableMetrics, StringJoiner sj) {
        sj.add("Table regions: " + tableMetrics.size() + ", full size: " + String.format("%.1f Gb", (float) getTableSize(tableMetrics).get() / MB));
    }

    private AtomicLong getTableSize(Map<String, RegionMetrics> tableMetrics) {
        AtomicLong size = new AtomicLong(0L);
        tableMetrics.values().forEach(rm -> {
            int regionSize = (int) rm.getStoreFileSize().getLongValue();
            size.addAndGet(regionSize);
        });
        return size;
    }

    private void calculateSize(Map<String, RegionMetrics> tableMetrics, StringJoiner sj) {
        Map<Integer, AtomicInteger> sizeHist = new HashMap<>();
        tableMetrics.values().forEach(rm -> {
            int regionSize = (int) rm.getStoreFileSize().getLongValue();

            int size = regionSize / MB;
            sizeHist.putIfAbsent(size, new AtomicInteger(0));
            sizeHist.get(size).incrementAndGet();
        });

        List<Integer> list = new ArrayList<>(sizeHist.keySet());
        Collections.sort(list);
        sj.add("Size histogram:");
        sj.add("(size: regions)");
        for (Integer i : list) {
            sj.add(i + ".." + (i + 1) + "Gb: " + sizeHist.get(i));
        }
        sj.add("");
    }

    private void calculateDistribution(TableName tableName, StringJoiner sj) throws IOException {
        List<HRegionLocation> regions = admin.getConnection().getRegionLocator(tableName).getAllRegionLocations();

        Map<ServerName, AtomicInteger> map = new HashMap<>();
        regions.forEach(regionLocation -> {
            map.putIfAbsent(regionLocation.getServerName(), new AtomicInteger());
            map.get(regionLocation.getServerName()).incrementAndGet();
        });

        Map<Integer, AtomicInteger> map2 = new HashMap<>();
        for (ServerName sn : map.keySet()) {
            map2.putIfAbsent(map.get(sn).get(), new AtomicInteger());
            map2.get(map.get(sn).get()).incrementAndGet();
        }

        List<Integer> tmp = new ArrayList<>(map2.keySet());
        Collections.sort(tmp);
        sj.add("Distribution histogram:");
        sj.add("(regions count: HBase servers)");
        for (Integer i : tmp) {
            sj.add(i + ": " + map2.get(i));
        }
        sj.add("");
    }

    private void calculateLocality(Map<String, RegionMetrics> tableMetrics, StringJoiner sj) {
        Map<Integer, AtomicInteger> sizeHist = new HashMap<>();
        tableMetrics.values().forEach(rm -> {
            float locality = rm.getDataLocality();

            int key = (int) (locality * 10) * 10;
            sizeHist.putIfAbsent(key, new AtomicInteger(0));
            sizeHist.get(key).incrementAndGet();
        });

        List<Integer> list = new ArrayList<>(sizeHist.keySet());
        Collections.sort(list);

        sj.add("Locality histogram:");
        sj.add("(locality: regions)");
        for (Integer i : list) {
            if (i == 100) {
                sj.add("1: " + sizeHist.get(i));
            } else {
                sj.add(String.format("%.1f..%.1f", (float) i / 100, (float) (i + 10) / 100) + ": " + sizeHist.get(i));
            }
        }
        sj.add("");
    }

    private void calculateFileCounts(Map<String, RegionMetrics> tableMetrics, StringJoiner sj) {
        Map<String, Integer> map = new HashMap<>();
        tableMetrics.values().forEach(rm -> map.put(rm.getNameAsString(), rm.getStoreFileCount()));

        Map<Integer, AtomicInteger> map2 = new HashMap<>();
        map.values().stream().distinct().forEach(v -> map2.put(v, new AtomicInteger()));

        map.forEach((k, v) -> map2.get(v).incrementAndGet());

        List<Integer> list = new ArrayList<>(map2.keySet());
        Collections.sort(list);

        sj.add("Files count:");
        sj.add("(count: regions)");
        for (Integer i : list) {
            sj.add(i + ": " + map2.get(i));
        }
    }

    private Map<String, RegionMetrics> getTableLoad(ClusterMetrics metrics, TableName tableName) throws IOException {
        Map<String, RegionMetrics> result = new HashMap<>();

        List<RegionInfo> regions = admin.getRegions(tableName);
        LOGGER.info("There are {} regions in table (by admin API)", regions.size());

        Map<ServerName, ServerMetrics> serverMetricsMap = metrics.getLiveServerMetrics();
        Map<String, RegionMetrics> regionMetrics = new HashMap<>();
        serverMetricsMap.forEach((k, v) -> {
            for (RegionMetrics rm : v.getRegionMetrics().values()) {
                String encodedRegionName = RegionInfo.getRegionNameAsString(rm.getRegionName());
                regionMetrics.put(encodedRegionName, rm);
            }
        });

        for (RegionInfo info : regions) {
            String encodedRegionName = RegionInfo.getRegionNameAsString(info.getRegionName());
            RegionMetrics rm = regionMetrics.get(encodedRegionName);
            if (rm != null) {
                result.put(encodedRegionName, rm);

                // double-check
                if (!Arrays.equals(rm.getRegionName(), info.getRegionName())) {
                    LOGGER.warn("Region names are not the same: {} != {}", rm.getRegionName(), info.getRegionName());
                }
            }
        }

        return result;
    }
}
