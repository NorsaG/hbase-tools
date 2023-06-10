package org.evla.hbase.distributor;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class DistributorRunner implements HBaseToolRunner {

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null || args.length == 0) {
            new DistributorService(admin, settings).run();
        } else if (args.length == 1) {
            new Distributor(admin, settings).run(TableName.valueOf(args[0]));
        } else if ("mask".equals(args[1])) {
            new Distributor(admin, settings).run(args[0]);
        }
    }
}
