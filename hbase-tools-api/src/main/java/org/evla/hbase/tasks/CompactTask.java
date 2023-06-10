package org.evla.hbase.tasks;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.rstask.RSTask;
import org.evla.hbase.compactor.CompactionWeight;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.rstask.RSTaskControllerHelper;

import java.util.concurrent.atomic.AtomicInteger;

public class CompactTask extends RSTask<Void> implements Comparable<CompactTask> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactTask.class);

    private ServerName sn;
    private HRegionInfo info;
    private Admin admin;
    private HBaseToolsSettings settings;

    private AtomicInteger counter;

    private CompactionWeight weight;

    CompactTask(ServerName sn, Admin admin, HRegionInfo info, HBaseToolsSettings settings, CompactionWeight weight, AtomicInteger counter) {
        this.sn = sn;
        this.admin = admin;
        this.info = info;
        this.settings = settings;
        this.weight = weight;
        this.counter = counter;
    }

    @Override
    public Void executeTask() {
        try {
            if (info != null) {
                LOGGER.info("[{}] Start compaction for: {}, weight = {}", sn.getServerName(), info.getRegionNameAsString(), String.format("%.2f", weight.calculateRegionCompactionWeight()));
                try {
                    admin.majorCompactRegion(info.getRegionName());
                    RSTaskControllerHelper.waitUntilCompacting(admin, info, settings.getCompactorSettings().getStatusDelay());
                    LOGGER.info("[{}] Compaction done for: {}, old weight = {}", sn.getServerName(), info.getRegionNameAsString(), String.format("%.2f", weight.calculateRegionCompactionWeight()));
                    //todo: проверить, насколько получение RegionLoad тяжелая операция, возможно, её можно использовать постоянно
                } catch (Exception exc) {
                    LOGGER.info("[" + sn.getServerName() + "] Compaction failed for: " + info.getRegionNameAsString(), exc);
                }
                counter.incrementAndGet();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public int compareTo(CompactTask task) {
        return this.weight.calculateRegionCompactionWeight().compareTo(task.weight.calculateRegionCompactionWeight());
    }
}
