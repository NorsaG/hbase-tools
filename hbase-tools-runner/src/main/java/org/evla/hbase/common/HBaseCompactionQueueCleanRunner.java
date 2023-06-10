package org.evla.hbase.common;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class HBaseCompactionQueueCleanRunner implements HBaseToolRunner {
    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        HBaseCompactionQueueCleaner cleaner = new HBaseCompactionQueueCleaner();
        cleaner.cleanQueues(admin);
    }
}
