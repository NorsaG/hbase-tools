package org.evla.hbase.merger;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class MergerService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergerService.class);

    private final Admin admin;

    private final QualityMerge qualityMerge = QualityMerge.SMALL;
    private final MergeParams mergeParams;

    public MergerService(Admin admin, Properties properties) {
        this.admin = admin;

        long minStorefileSize_mb = Integer.parseInt(properties.getProperty("merger.regions.min-storefile-size-mb", "32"));
        long maxStorefileSize_mb = Integer.parseInt(properties.getProperty("merger.regions.max-storefile-size-mb", "6124"));
        long maxMergedStorefileSize_mb = Integer.parseInt(properties.getProperty("merger.regions.max-merged-storefile-size-mb", "8192"));

        this.mergeParams = new MergeParams(minStorefileSize_mb, maxStorefileSize_mb, maxMergedStorefileSize_mb);
    }

    public void run(String namespace, boolean withMerge) {
        runAnalyze(namespace, withMerge);
    }

    private void runAnalyze(String namespace, boolean withMerge) {
        ConcurrentMap<TableName, TreeSet<byte[]>> allRegions = new ConcurrentHashMap<>();
        List<TableName> tables = new ArrayList<>();
        analyzeClusterForSmallMerge(namespace, allRegions, tables);
        if (withMerge) {
            runMerge(tables, allRegions);
            analyzeClusterForSmallMerge(namespace, allRegions, tables);
        }
    }

    private void analyzeClusterForSmallMerge(String namespace, ConcurrentMap<TableName, TreeSet<byte[]>> allRegions, List<TableName> tables) {
        allRegions.clear();
        tables.clear();

        ConcurrentMap<TableName, TreeSet<byte[]>> allSmallRegions = new ConcurrentHashMap<>();
        ConcurrentMap<TableName, AtomicLong> tablesSize = new ConcurrentHashMap<>();
        try {
            ClusterStatus status = admin.getClusterStatus();
            status.getServers().parallelStream().forEach(sn -> {
                try {
                    status.getLoad(sn).getRegionsLoad().forEach((k, v) -> {
                        TableName tn = HRegionInfo.getTable(v.getName());
                        if (tn.getNamespaceAsString().equals(namespace)) {
                            tablesSize.putIfAbsent(tn, new AtomicLong(0L));
                            tablesSize.get(tn).addAndGet(1);
                            allRegions.putIfAbsent(tn, new TreeSet<>(Bytes.BYTES_COMPARATOR));
                            allRegions.get(tn).add(v.getName());
                            if (v.getStorefileSizeMB() <= mergeParams.getMinStorefileSize_mb()) {
                                allSmallRegions.putIfAbsent(tn, new TreeSet<>(Bytes.BYTES_COMPARATOR));
                                allSmallRegions.get(tn).add(v.getName());
                            }
                        }
                    });
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    throw new RuntimeException("Analyze failed. Check all logs... " + e.getMessage(), e);
                }
            });
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }

        LOGGER.info("There are {} tables with {} small regions in cluster", allSmallRegions.size(), allSmallRegions.values().stream().mapToInt(Collection::size).sum());
        tables.addAll(allSmallRegions.keySet().parallelStream().filter(tn -> {
            try {
                return admin.getConnection().getRegionLocator(tn).getAllRegionLocations().size() > 1;
            } catch (IOException ex) {
                return false;
            }
        }).collect(Collectors.toList()));
        AtomicLong counter = new AtomicLong(0L);

        tables.parallelStream().forEach(tn -> {
            long allRegionsCount = tablesSize.get(tn).get();
            long smallRegions = allSmallRegions.get(tn).size();
            if (allRegionsCount == smallRegions) {
                counter.addAndGet(allRegionsCount - 1);
            } else {
                counter.addAndGet(smallRegions);
            }
        });
        LOGGER.info("There are {} regions in cluster that can be merged by MergerService", counter.get());
    }

    private void runMerge(List<TableName> preparedTables, ConcurrentMap<TableName, TreeSet<byte[]>> allRegions) {
        List<TableName> biggest = preparedTables.stream().sorted(Comparator.comparingLong(tn -> allRegions.get(tn).size())).collect(Collectors.toList());
        Collections.reverse(biggest);
        Merger merger = new Merger(admin, qualityMerge, mergeParams);
        for (TableName tn : biggest) {
            try {
                merger.mergeTable(tn);
            } catch (Exception e) {
                LOGGER.error("Cant merge table " + tn, e);
            }
        }
    }
}
