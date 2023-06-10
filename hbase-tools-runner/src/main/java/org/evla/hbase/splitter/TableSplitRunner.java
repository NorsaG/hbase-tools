package org.evla.hbase.splitter;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class TableSplitRunner implements HBaseToolRunner {

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null || args.length != 2) {
            throw new RuntimeException();
        }
        new TableSplitter(admin, settings).splitTable(args[0], Integer.parseInt(args[1]));
    }
}
