package org.evla.hbase.merger;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.rstask.RSTaskControllerHelper;
import org.evla.hbase.configuration.HBaseToolsSettings;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Tool for merge automation.
 * There are 3 modes:
 * <p>
 * </p>
 * <i>merger.quality=small</i>
 * <p>
 * <p>
 * Do merge for all regions those less then merger.regions.min-storefile-size-mb with adjacent regions.
 * One merge iteration.
 *
 * <p>
 * </p>
 * <i>merger.quality=medium</i>
 * <p>
 * <p>
 * Try to merge all regions while region`s count more than merger.regions.border (in some case in result can be more than merger.regions.border)
 * Several merge iterations.
 * <p>
 * In case of big regions merge process will be stopped.
 * <p>
 * <p>
 * </p>
 * <i>merger.quality=large</i>
 * <p>
 * Merge all while we can.
 * <p>
 * </p>
 * <p>
 * Borders and rules are (with default values):
 * <p>
 * </p>
 * <i> merger.regions.min-storefile-size-mb=64 </i>
 * <p>
 * <p>
 * Minimal region size. Will merge all regions (those less then border)
 * <p>
 * </p>
 * <i>merger.regions.max-storefile-size-mb=6124 </i>
 * <p>
 * Maximum region`s size.
 * If more than border - skip it (except case when region`s size smaller than merger.regions.min-storefile-size-mb)
 * <p>
 * <i>merger.regions.max-merged-storefile-size-mb=8192</i>
 * <p>
 * Approximately size of new regions (calculate as sum of two region`s sizes) - result region size can`t be greater.
 */
public class Merger {
    private static final Logger LOGGER = LoggerFactory.getLogger(Merger.class);
    private static final int MAX_RETRIES = 10;

    private final Admin admin;

    private final QualityMerge qualityMerge;
    private final MergeParams mergeParams;

    private final List<MergedRegion> oneStepRegions = new ArrayList<>();
    boolean isCheckSnp = true;

    public Merger(Admin admin, QualityMerge qualityMerge, MergeParams mergeParams) {
        this.admin = admin;
        this.qualityMerge = qualityMerge;
        this.mergeParams = mergeParams;
    }

    public Merger(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.qualityMerge = QualityMerge.parseQuality(settings.getMergerSettings().getMergerQuality());

        final long minStorefileSize_mb = settings.getMergerSettings().getMergerMinStoreFileSize();
        final long maxStorefileSize_mb = settings.getMergerSettings().getMergerMaxStoreFileSize();
        final long maxMergedStorefileSize_mb = settings.getMergerSettings().getMergerMaxMergedStoreFileSize();

        this.mergeParams = new MergeParams(minStorefileSize_mb, maxStorefileSize_mb, maxMergedStorefileSize_mb);
        this.isCheckSnp = settings.getMergerSettings().isCheckSnapshot();
    }

    public void run(String table, int border) {
        run(table, border, isCheckSnp);
    }

    public void run(String table, int border, boolean checkSnapshotExists) {
        TableName tableName = TableName.valueOf(table);
        if (checkSnapshotExists) {
            checkSnapshots(tableName);
        }

        this.qualityMerge.setBorderRegionsCount(border);

        LOGGER.info("Start merge process for {}", table);
        try {
            mergeTable(tableName);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        LOGGER.info("Stop merging table {}. Current region count: {}", tableName, getRegionCount(tableName));
    }

    private void checkSnapshots(TableName tableName) {
        try {
            List<SnapshotDescription> descriptions = admin.listSnapshots();
            for (SnapshotDescription d : descriptions) {
                TableName tnSNP = TableName.valueOf(d.getTableNameAsString());
                if (tableName.equals(tnSNP)) {
                    throw new RuntimeException("Table has snapshot. Merge is forbidden. For force merge run merger with 'merger.checkSnapshotExists' = false flag.");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Cant get info about table snapshots", e);
        }
    }

    void mergeTable(TableName t) throws Exception {
        oneStepRegions.clear();
        if (qualityMerge == QualityMerge.SMALL) {
            LOGGER.info("Start small merge for {}", t);
            tryMergeTableRegions(t);
        } else {
            int currentRegionsCount = getRegionCount(t);
            while (currentRegionsCount > qualityMerge.getBorderRegionsCount()) {
                LOGGER.info("Current table regions count: {}. Start next merging step for {}", currentRegionsCount, t);
                boolean merging = tryMergeTableRegions(t);
                if (merging) {
                    waiting();
                } else {
                    LOGGER.info("No regions for merge. Stop merging.");
                    break;
                }
                currentRegionsCount = admin.getRegions(t).size();
            }
        }
    }

    private int getRegionCount(TableName tableName) {
        try {
            return admin.getRegions(tableName).size();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return -1;
        }
    }

    private void waiting() {
        LOGGER.info("Sleep before next merge step");
        RSTaskControllerHelper.sleep(5_000);
    }

    private Map<String, RegionMetrics> getTableMetrics(TableName tableName) throws IOException {
        Map<String, RegionMetrics> result = new HashMap<>();
        ClusterMetrics metrics = admin.getClusterMetrics();

        Collection<ServerName> serverNames = metrics.getServersName();
        List<RegionInfo> regions = admin.getRegions(tableName);
        Map<ServerName, ServerMetrics> serverMetricsMap = metrics.getLiveServerMetrics();
        serverNames.parallelStream().forEach(sn -> {
            try {
                ServerMetrics serverMetrics = serverMetricsMap.get(sn);
                for (RegionInfo info : regions) {
                    RegionMetrics regionMetrics = serverMetrics.getRegionMetrics().get(info.getRegionName());
                    if (regionMetrics != null) {
                        result.put(Bytes.toStringBinary(regionMetrics.getRegionName()), regionMetrics);
                    }
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        });

        return result;
    }

    private boolean tryMergeTableRegions(TableName t) throws Exception {
        boolean regionsMerged = false;

        Map<String, RegionMetrics> allRegionsOfTable = getTableMetrics(t);
        Map<String, RegionInfo> allRegionsInfo = new HashMap<>();

        List<HRegionLocation> locations = admin.getConnection().getRegionLocator(t).getAllRegionLocations();
        List<byte[]> tableRegions = new ArrayList<>();
        for (HRegionLocation loc : locations) {
            tableRegions.add(loc.getRegion().getRegionName());
            allRegionsInfo.put(loc.getRegion().getEncodedName(), loc.getRegion());
        }
        tableRegions.sort(Bytes.BYTES_COMPARATOR);
        List<String> sortedRegions = tableRegions.stream().map(Bytes::toStringBinary).collect(Collectors.toList());

        for (int i = 0; i < sortedRegions.size() - 1; i++) {
            RegionMetrics a = allRegionsOfTable.get(sortedRegions.get(i));
            RegionMetrics b = allRegionsOfTable.get(sortedRegions.get(i + 1));

            if (a == null) {
                LOGGER.info("Null region metrics: {}", sortedRegions.get(i));
                continue;
            } else if (b == null) {
                LOGGER.info("Null region metrics: {}", sortedRegions.get(i + 1));
                continue;
            } else if (qualityMerge.canMergeRegions(a, b, mergeParams)) {
                if (RegionInfo.areAdjacent(allRegionsInfo.get(RegionInfo.encodeRegionName(a.getRegionName())), allRegionsInfo.get(RegionInfo.encodeRegionName(b.getRegionName())))) {
                    LOGGER.info("Start merge regions {} and {}", a.getNameAsString(), b.getNameAsString());
                    try {
                        admin.mergeRegionsAsync(a.getRegionName(), b.getRegionName(), false);
                    } catch (UnknownRegionException e) {
                        LOGGER.warn("There is unknown region in table. Waiting for correct regions. {}", e.getMessage());
                        regionsMerged = true;
                        break;
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                        try {
                            admin.mergeRegionsAsync(b.getRegionName(), a.getRegionName(), false);
                        } catch (Exception exc) {
                            LOGGER.error(exc.getMessage(), exc);
                        }
                    }
                    oneStepRegions.add(MergedRegion.from(allRegionsInfo.get(RegionInfo.encodeRegionName(a.getRegionName())), allRegionsInfo.get(RegionInfo.encodeRegionName(b.getRegionName()))));
                    regionsMerged = true;
                    i++;
                } else {
                    LOGGER.warn("Regions {} and {} are not adjacent", a.getNameAsString(), b.getNameAsString());
                }
            }
            int currentRegionCount = sortedRegions.size() - oneStepRegions.size();
            if (regionsMerged && qualityMerge == QualityMerge.MEDIUM && currentRegionCount <= qualityMerge.getBorderRegionsCount()) {
                LOGGER.info("Stop merging.");
                break;
            }
        }
        try {
            LOGGER.info("Waiting for transitions...");
            // Ждем, пока таблица находится в транзите
            waitUntilTransition(t);
        } catch (Exception e) {
            LOGGER.warn("Exception while transitions in: " + t + ", " + e.getMessage(), e);
        }

        LOGGER.info("Waiting for compactions...");
        // Ждем, пока таблица компактифицируется
        RSTaskControllerHelper.waitUntilCompacting_checked(admin, t, 5_000);
        return regionsMerged;
    }

    private void waitUntilTransition(TableName t) throws Exception {
        if (oneStepRegions.isEmpty()) {
            return;
        }
        try {
            List<HRegionLocation> tableRegions = admin.getConnection().getRegionLocator(t).getAllRegionLocations();
            int mergedRegionsSize = oneStepRegions.size();
            for (int i = 0; i < MAX_RETRIES; i++) {
                List<RegionInfo> mergedRegions = findNewRegions(oneStepRegions, tableRegions);
                boolean isRegionsNotReady = false;
                Map<String, RegionState> regionsInTransition = RSTaskControllerHelper.getRegionsInTransitionMap(admin.getClusterMetrics());
                for (RegionInfo info : mergedRegions) {
                    if (regionsInTransition.containsKey(info.getEncodedName())) {
                        LOGGER.info("Not all regions ready. Waiting...");
                        RSTaskControllerHelper.sleep(5_000);
                        isRegionsNotReady = true;
                        break;
                    }
                }
                if (!isRegionsNotReady && mergedRegions.size() == mergedRegionsSize) {
                    break;
                } else if (mergedRegions.size() != mergedRegionsSize) {
                    RSTaskControllerHelper.sleep(5_000);
                }
            }
        } catch (Exception e) {
            LOGGER.info(e.getMessage(), e);
        }
        oneStepRegions.clear();
    }

    private List<RegionInfo> findNewRegions(List<MergedRegion> oneStepRegions, List<HRegionLocation> tableRegions) {
        List<RegionInfo> mergedRegions = new ArrayList<>();
        for (HRegionLocation loc : tableRegions) {
            RegionInfo info = loc.getRegion();
            for (MergedRegion mr : oneStepRegions) {
                if (Arrays.equals(info.getStartKey(), mr.getStartKey()) && Arrays.equals(info.getEndKey(), mr.getEndKey())) {
                    mergedRegions.add(info);
                }
            }
        }
        return mergedRegions;
    }

    private boolean isRegionReady(RegionState rs) {
        return rs.isOpened();
    }

}
