package org.evla.hbase.compactor;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.configuration.CompactorSettings;
import org.evla.hbase.rstask.RSTaskControllerHelper;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionTask implements Callable<Boolean>, Comparable<CompactionTask> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionTask.class);

    private final ServerName sn;
    private final HRegionInfo info;
    private final Admin admin;
    private final CompactorSettings settings;
    private final AtomicInteger counter;
    private final AtomicInteger totalVolume;
    private final CompactionWeight weight;

    CompactionTask(ServerName sn, Admin admin, HRegionInfo info, CompactorSettings settings, CompactionWeight weight, AtomicInteger counter, AtomicInteger totalVolume) {
        this.sn = sn;
        this.admin = admin;
        this.info = info;
        this.settings = settings;
        this.weight = weight;
        this.counter = counter;
        this.totalVolume = totalVolume;
    }

    @Override
    public Boolean call() {
        if (weight == null) {
            return compactEmptyWeightRegion(info);
        } else {
            return compactRegion(info);
        }
    }

    private boolean doCompact(HRegionInfo info) {
        try {
            admin.majorCompactRegion(info.getRegionName());
            return true;
        } catch (Exception e) {
            LOGGER.info("[" + sn.getServerName() + "] Compaction failed for: " + info.getRegionNameAsString(), e);
            return false;
        }
    }

    private boolean compactRegion(HRegionInfo info) {
        if (info != null) {
            LOGGER.info("[{}] Start compaction for: {}, weight = {}", sn.getServerName(), info.getRegionNameAsString(), String.format("%.2f", weight.calculateRegionCompactionWeight()));
            try {
                boolean result = doCompact(info);
                if (result) {
                    RSTaskControllerHelper.waitUntilCompacting(admin, info, settings.getStatusDelay());
                    totalVolume.addAndGet(weight.getTotalRegionSize());
                    LOGGER.info("[{}] Compaction done for: {}, old weight = {}", sn.getServerName(), info.getRegionNameAsString(), String.format("%.2f", weight.calculateRegionCompactionWeight()));
                    counter.incrementAndGet();
                    return true;
                } else {
                    return false;
                }
            } catch (Exception exc) {
                LOGGER.info("[" + sn.getServerName() + "] Exception while compacting: " + info.getRegionNameAsString(), exc);
            }
        }
        return false;
    }

    private boolean compactEmptyWeightRegion(HRegionInfo info) {
        if (info != null) {
            LOGGER.info("[{}] Start compaction for: {}", sn.getServerName(), info.getRegionNameAsString());
            try {
                boolean result = doCompact(info);
                if (result) {
                    RSTaskControllerHelper.waitUntilCompacting(admin, info, settings.getStatusDelay());
                    LOGGER.info("[{}] Compaction done for: {} ", sn.getServerName(), info.getRegionNameAsString());
                    counter.incrementAndGet();
                    return true;
                } else {
                    return false;
                }
            } catch (Exception exc) {
                LOGGER.info("[" + sn.getServerName() + "] Exception while compacting: " + info.getRegionNameAsString(), exc);
            }
        }
        return false;
    }

    public HRegionInfo getInfo() {
        return info;
    }

    @Override
    public int compareTo(CompactionTask task) {
        if (weight == null || task.weight == null) {
            return 0;
        }
        return this.weight.calculateRegionCompactionWeight().compareTo(task.weight.calculateRegionCompactionWeight());
    }
}
