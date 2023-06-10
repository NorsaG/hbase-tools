package org.evla.hbase.common;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class TableCopyRunner implements HBaseToolRunner {
    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null || args.length != 2) {
            throw new RuntimeException();
        }
        TableName originalTable = TableName.valueOf(args[0]);
        String newTableName = args[1];
        HBaseStaticHelper.createTableCopy(admin, originalTable, newTableName);
    }
}
