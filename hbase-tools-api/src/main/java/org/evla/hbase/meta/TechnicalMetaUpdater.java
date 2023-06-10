package org.evla.hbase.meta;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Table;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class TechnicalMetaUpdater {
    private static final Logger LOGGER = LoggerFactory.getLogger(TechnicalMetaUpdater.class);
    private static final String LOCK_NAME = "META_UPDATE";

    private static ScheduledExecutorService service = null;

    private TechnicalMetaUpdater() {
    }

    public static void updateTechnicalMeta(Admin admin, HBaseToolsSettings settings, String runner, int pid) {
        runUpdater(admin, settings, runner, pid);
    }

    private static void runUpdater(Admin admin, HBaseToolsSettings settings, String runner, int pid) {
        try {
            Table lockTable = admin.getConnection().getTable(settings.getLockSettings().getLockTable());
            TableLock lock = new TableLock(lockTable, LOCK_NAME, pid, runner, settings.getLockSettings().getDefaultLockTTL());

            TechnicalMeta technicalMeta = new TechnicalMeta(admin, settings);
            AtomicBoolean flag = new AtomicBoolean(false);
            service = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("meta-updater").build());
            service.scheduleWithFixedDelay(() -> {
                boolean isLockOwner = flag.get();
                LOGGER.info("Try to acquire lock. Current ownership: {}", isLockOwner);
                if (Boolean.TRUE.equals(isLockOwner)) {
                    lock.updateLock();
                    technicalMeta.getActualTopology(true);
                } else {
                    boolean isLocked = lock.acquireLock();
                    flag.set(isLocked);
                    if (isLocked) {
                        technicalMeta.getActualTopology(true);
                    }
                }
            }, 0, settings.getTechnicalMetaSettings().getTechnicalMetaScanPeriod(), TimeUnit.SECONDS);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public static void stopUpdater() {
        if (service != null) {
            service.shutdown();
        }
    }
}
