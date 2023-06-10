package org.evla.hbase.flusher;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.ClusterTaskController;
import org.evla.hbase.rstask.RSTask;
import org.evla.hbase.rstask.RSTaskController;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.tasks.FlushTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.rstask.RSTaskControllerHelper;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

public class FlushController extends ClusterTaskController<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTaskController.class);

    private final Admin admin;
    private final int threads;
    private final int memoryBorder;

    FlushController(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.threads = settings.getFlusherSettings().getFlusherThreads();
        this.memoryBorder = settings.getFlusherSettings().getFlusherMemstoreBorder();
    }

    @Override
    public void initTasks(Map<ServerName, RSTaskController<Void>> controllers) {
        try {
            Map<ServerName, List<FlushTask>> tasks = new HashMap<>();
            ClusterStatus status = admin.getClusterStatus();
            for (ServerName sn : status.getServers()) {
                tasks.putIfAbsent(sn, new ArrayList<>());
                if (servers == null || servers.contains(sn)) {
                    ServerLoad serverLoad = status.getLoad(sn);
                    for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
                        RegionLoad load = entry.getValue();
                        if (load.getMemStoreSizeMB() > memoryBorder)
                            tasks.get(sn).add(new FlushTask(admin, load.getName(), load.getMemStoreSizeMB()));
                    }
                }
            }
            tasks.values().forEach(tl -> tl.sort(Collections.reverseOrder()));

            controllers.values().forEach(c -> {
                List<? extends RSTask<Void>> taskList = tasks.get(c.getServerName());
                c.addAllTask(taskList);
            });
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public Map<ServerName, RSTaskController<Void>> getControllers() {
        return RSTaskControllerHelper.getControllers(admin, threads);
    }

    @Override
    public Supplier<Void> getRSThreadRunner(ServerName sn, RSTaskController<Void> controller) {
        return () -> {
            LOGGER.info("{}: {} regions for flush", sn, controller.getTasksCount());
            controller.processTasks();
            return null;
        };
    }

    @Override
    public String type() {
        return "Flusher";
    }
}
