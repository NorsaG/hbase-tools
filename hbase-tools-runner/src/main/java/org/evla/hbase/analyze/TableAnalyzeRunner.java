package org.evla.hbase.analyze;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

import java.io.File;

public class TableAnalyzeRunner implements HBaseToolRunner {

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Incorrect input parameters");
        }

        String input = args[0];
        if (args.length == 1) {
            new TableAnalyzer(admin).run(TableName.valueOf(input));
            return;
        }

        String parameter = args[1];
        if ("file".equals(parameter)) {
            new TableAnalyzer(admin).run(new File(input));
        } else if ("mask".equals(parameter)) {
            new TableAnalyzer(admin).run(input);
        }

    }
}
