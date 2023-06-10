package org.evla.hbase.common;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class TableReplaceRunner implements HBaseToolRunner {
    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null || args.length != 3) {
            throw new RuntimeException();
        }
        TableName from = TableName.valueOf(args[0]);
        TableName to = TableName.valueOf(args[1]);
        boolean withCopy = Boolean.parseBoolean(args[2]);
        HBaseStaticHelper.replaceTable(admin, from, to, withCopy);
    }
}
