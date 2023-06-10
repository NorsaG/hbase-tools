package org.evla.hbase.splitter;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.evla.hbase.rstask.RSTaskControllerHelper;
import org.evla.hbase.compactor.LightweightCompactor;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.distributor.Distributor;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TableSplitter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableSplitter.class);

    private static final int RETRIES = 20;
    private final Admin admin;

    private final double divideMultiplier;
    private final double splitMultiplier;
    private final boolean isNeedToDistribute;

    private final HBaseToolsSettings settings;

    public TableSplitter(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.settings = settings;
        this.splitMultiplier = settings.getSplitterSettings().getSplitMultiplier();
        this.divideMultiplier = settings.getSplitterSettings().getSplitDivideMultiplier();

        this.isNeedToDistribute = settings.getSplitterSettings().isDistributeAfterSplit();
    }

    public void splitTable(String tableName, int newSize) {
        TableName tn = TableName.valueOf(tableName);
        Distributor distributor = new Distributor(admin, settings);
        List<HRegionLocation> regions = new ArrayList<>();
        LightweightCompactor compactor = null;
        try {
            int currentSize = getTableRegionsCount(tn);
            if (currentSize >= newSize) {
                throw new RuntimeException("Current table regions count more or equal new size");
            }
            LOGGER.info("Current regions for table {}: {}", tableName, currentSize);
            int globalSplitStep = 1;
            while (currentSize < newSize) {
                RSTaskControllerHelper.waitUntilRegionsReadyToSplit(admin, tn);
                LOGGER.info("Run {} step of splitting for table {}", globalSplitStep, tableName);
                if (currentSize * 2 * splitMultiplier < newSize) {
                    LOGGER.info("Split full table {}", tableName);

                    List<HRegionLocation> locations = admin.getConnection().getRegionLocator(tn).getAllRegionLocations();
                    for (HRegionLocation loc : locations) {
                        splitRegion(regions, loc);
                    }
                } else {
                    LOGGER.info("Start split biggest regions of table {}", tableName);
                    splitBiggestRegions(regions, tn, newSize - currentSize);
                }
                if (!regions.isEmpty()) {
                    if (compactor == null) {
                        compactor = new LightweightCompactor(admin, settings);
                    }
                    compactor.compactRegions(new ArrayList<>(regions));
                    RSTaskControllerHelper.waitUntilCompacting_checked(admin, tn, 3_000);
                    regions.clear();
                }
                RSTaskControllerHelper.waitWhileSplitting(admin, tn);
                if (isNeedToDistribute) {
                    distributor.run(tableName);
                }
                currentSize = getTableRegionsCount(tn);
                LOGGER.info("Current regions for table {}: {}", tableName, currentSize);
                globalSplitStep++;
                if (globalSplitStep > RETRIES) {
                    LOGGER.warn("Too much steps during splitting");
                    break;
                }
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (compactor != null) {
                compactor.stopCompact();
            }
        }
        LOGGER.info("Table split to {} regions", getTableRegionsCount(tn));
    }

    private void splitRegion(List<HRegionLocation> regions, HRegionLocation location) throws IOException {
        byte[] start = location.getRegion().getStartKey();
        byte[] end = location.getRegion().getEndKey();
        byte xFF = (byte) 0xFF;

        if (Arrays.equals(end, new byte[0])) {
            end = new byte[]{xFF, xFF, xFF, xFF, xFF, xFF, xFF, xFF};
        }

        byte[][] split = Bytes.split(start, end, 1);
        byte[] middle = split[1];

        if (RSTaskControllerHelper.isRegionSplittable(admin, location)) {
            admin.splitRegionAsync(location.getRegion().getRegionName(), middle);
        } else {
            regions.add(location);
        }
    }

    private void splitBiggestRegions(List<HRegionLocation> regions, TableName tn, int splitCount) throws IOException {
        List<HRegionLocation> locations = admin.getConnection().getRegionLocator(tn).getAllRegionLocations();
        Map<String, HRegionLocation> allLocations = locations.stream().collect(Collectors.toMap(l -> RegionInfo.getRegionNameAsString(l.getRegion().getRegionName()), l -> l));
        List<RegionMetrics> loads = getSortedRegionLoads(tn);
        if (loads.size() < 1) {
            LOGGER.info("Region loads are empty...");
            return;
        }

        int maxSize = (int) loads.get(0).getStoreFileSize().getLongValue();
        for (int i = 0; i < splitCount; i++) {
            int rSize = Math.max(1, (int) loads.get(i).getStoreFileSize().getLongValue());
            if (rSize * 2 * divideMultiplier > maxSize) {
                try {
                    Thread.sleep(1_000);
                    String regionName = RegionInfo.getRegionNameAsString(loads.get(i).getRegionName());
                    HRegionLocation location = allLocations.get(regionName);
                    if (RSTaskControllerHelper.isRegionSplittable(admin, location)) {
                        splitRegion(regions, location);
                        LOGGER.info("Run split for table {}. Region size: {} MB, region name {}", tn, loads.get(i).getStoreFileSize().getLongValue(), loads.get(i).getNameAsString());
                    } else {
                        LOGGER.info("Region {} marked as not splittable. Schedule it for compact", loads.get(i).getNameAsString());
                        regions.add(location);
                    }
                } catch (IOException e) {
                    LOGGER.warn("Can`t split region: " + loads.get(i).getNameAsString(), e);
                } catch (InterruptedException e) {
                    LOGGER.warn(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            } else {
                break;
            }
        }
    }

    private List<RegionMetrics> getSortedRegionLoads(TableName tn) {
        try {
            List<RegionMetrics> regionLoads = new ArrayList<>();
            List<HRegionLocation> tableRegionInfos = admin.getConnection().getRegionLocator(tn).getAllRegionLocations();
            Set<byte[]> tableRegions;
            tableRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
            for (HRegionLocation regionInfo : tableRegionInfos) {
                tableRegions.add(regionInfo.getRegion().getRegionName());
            }
            ClusterMetrics status = admin.getClusterMetrics();
            for (ServerName serverName : status.getServersName()) {
                ServerMetrics serverMetrics = status.getLiveServerMetrics().get(serverName);
                for (RegionMetrics regionMetrics : serverMetrics.getRegionMetrics().values()) {
                    if (tableRegions.contains(regionMetrics.getRegionName())) {
                        regionLoads.add(regionMetrics);
                    }
                }
            }
            regionLoads.sort(Collections.reverseOrder(Comparator.comparingLong(rm -> rm.getStoreFileSize().getLongValue())));
            return regionLoads;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private int getTableRegionsCount(TableName tn) {
        try {
            return admin.getConnection().getRegionLocator(tn).getAllRegionLocations().size();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return -1;
    }
}
