package org.evla.hbase.common;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class KeyFinderRunner implements HBaseToolRunner {
    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null || args.length != 2) {
            throw new IllegalArgumentException("Incorrect input");
        }
        new KeyFinder(TableName.valueOf(args[0])).findLocation(admin, args[1]);
    }
}
