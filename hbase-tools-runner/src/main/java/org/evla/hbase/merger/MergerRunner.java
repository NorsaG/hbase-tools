package org.evla.hbase.merger;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class MergerRunner implements HBaseToolRunner {

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null) {
            throw new RuntimeException();
        }
        if (args.length == 3 && "safe".equals(args[2])) {
            new SafeMerger(admin, settings).run(args[0], Integer.parseInt(args[1]));
        } else {
            new Merger(admin, settings).run(args[0], Integer.parseInt(args[1]));
        }
    }
}
