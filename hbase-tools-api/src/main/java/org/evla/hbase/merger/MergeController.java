package org.evla.hbase.merger;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.evla.hbase.ClusterTaskController;
import org.evla.hbase.rstask.RSTaskController;
import org.evla.hbase.rstask.RSTaskControllerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

@Deprecated
public class MergeController extends ClusterTaskController<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeController.class);

    private final Admin admin;
    private final MergeParams mergeParams;

    public MergeController(Admin admin, Properties properties) {
        this.admin = admin;

        long minStorefileSize_mb = Integer.parseInt(properties.getProperty("merger.regions.min-storefile-size-mb", "32"));
        long maxStorefileSize_mb = Integer.parseInt(properties.getProperty("merger.regions.max-storefile-size-mb", "6124"));
        long maxMergedStorefileSize_mb = Integer.parseInt(properties.getProperty("merger.regions.max-merged-storefile-size-mb", "8192"));

        this.mergeParams = new MergeParams(minStorefileSize_mb, maxStorefileSize_mb, maxMergedStorefileSize_mb);
    }

    @Override
    public void initTasks(Map<ServerName, RSTaskController<Void>> controllers) {
        ConcurrentMap<ServerName, TreeSet<byte[]>> allSmallRegions = new ConcurrentHashMap<>();
        ConcurrentMap<TableName, AtomicLong> tablesSize = new ConcurrentHashMap<>();

        try {
            admin.getClusterStatus().getServers().parallelStream().forEach(sn -> {
                try {
                    allSmallRegions.putIfAbsent(sn, new TreeSet<>(Bytes.BYTES_COMPARATOR));
                    Set<byte[]> regions = allSmallRegions.get(sn);
                    admin.getClusterStatus().getLoad(sn).getRegionsLoad().forEach((k, v) -> {
                        TableName tn = HRegionInfo.getTable(v.getName());
                        tablesSize.putIfAbsent(tn, new AtomicLong(0L));
                        tablesSize.get(tn).addAndGet(1);
                        if (v.getStorefileSizeMB() <= mergeParams.getMinStorefileSize_mb()) {
                            regions.add(v.getName());
                        }
                    });
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                    throw new RuntimeException("Analyze failed. Check all logs... " + e.getMessage(), e);
                }
            });
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<ServerName, RSTaskController<Void>> getControllers() {
        return RSTaskControllerHelper.getControllers(admin, 1);
    }

    @Override
    public Supplier<Void> getRSThreadRunner(ServerName sn, RSTaskController<Void> controller) {
        return () -> {
            LOGGER.info("{}: Regions for merging: {}", sn, controller.getTasksCount());
            controller.processTasks();
            return null;
        };
    }

    @Override
    public String type() {
        return "Merger";
    }
}
