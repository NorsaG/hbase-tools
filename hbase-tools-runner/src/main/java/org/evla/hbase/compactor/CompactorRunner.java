package org.evla.hbase.compactor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.StaticConnector;
import org.evla.hbase.Tool;
import org.evla.hbase.analyze.ClusterAvailabilityObject;
import org.evla.hbase.analyze.HBaseHealthAnalyzeService;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.meta.MetaTableHolder;
import org.evla.hbase.meta.TechnicalMetaUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CompactorRunner implements HBaseToolRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactorRunner.class);

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        CompactorManager cm = new CompactorManager(admin, settings);
        try {
            if (args == null) {
                runHealthMonitor(settings, admin);
                runMetaUpdate(settings, admin);
                cm.infiniteCompact();
                Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(StaticConnector::refreshConnection, 1, 1, TimeUnit.DAYS);
                startDummyThread();
            } else {
                if (args[0].contains(":")) {
                    cm.compactTables(Collections.singletonList(TableName.valueOf(args[0])));
                } else {
                    cm.compactNamespaces(Collections.singleton(args[0]));
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void startDummyThread() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    LOGGER.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
            }
        }).start();
    }

    private void runMetaUpdate(HBaseToolsSettings settings, Admin admin) {
        if (settings.getTechnicalMetaSettings().isTechnicalMetaEnable()) {
            LOGGER.info("Start update technical meta");
            TechnicalMetaUpdater.updateTechnicalMeta(admin, settings, "compactor", Tool.getPID());
        }
    }

    private void runHealthMonitor(HBaseToolsSettings settings, Admin admin) {
        if (settings.getCheckerSettings().isCheckerEnable()) {
            HBaseHealthAnalyzeService healthAnalyzeService = new HBaseHealthAnalyzeService(admin, settings, new MetaTableHolder(), true);
            healthAnalyzeService.startChecking();

            ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("health-check").setDaemon(true).build());
            service.scheduleWithFixedDelay(() -> {
                ClusterAvailabilityObject object = healthAnalyzeService.getClusterAvailabilityObject();
                LOGGER.info("Cluster availability at {}: \n{}", LocalDateTime.now(), object.toString());
            }, settings.getCheckerSettings().getCheckInterval(), settings.getCheckerSettings().getCheckInterval(), TimeUnit.SECONDS);
        }
    }

}
