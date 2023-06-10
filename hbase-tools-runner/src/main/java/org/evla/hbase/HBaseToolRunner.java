package org.evla.hbase;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.configuration.HBaseToolsSettings;

public interface HBaseToolRunner {
    void run(Admin admin, HBaseToolsSettings settings, String... args);
}
