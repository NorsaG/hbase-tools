package org.evla.hbase.tasks;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.rstask.RSTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FlushTask extends RSTask<Void> implements Comparable<FlushTask> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlushTask.class);

    private final Admin admin;
    private final byte[] regionName;
    private final int memStoreSize;

    public FlushTask(Admin admin, byte[] regionName, int memStoreSize) {
        this.admin = admin;
        this.regionName = regionName;
        this.memStoreSize = memStoreSize;
    }

    @Override
    public Void executeTask() {
        try {
            LOGGER.info("Start flushing {} region ({} mb)", new String(regionName), memStoreSize);
            admin.flushRegion(regionName);
            LOGGER.info("Stop flushing region {}", new String(regionName));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public int compareTo(FlushTask o) {
        return Long.compare(memStoreSize, o.memStoreSize);
    }
}
