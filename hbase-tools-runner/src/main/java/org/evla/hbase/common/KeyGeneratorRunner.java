package org.evla.hbase.common;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class KeyGeneratorRunner implements HBaseToolRunner {
    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("Incorrect input");
        }
        KeyGenerator.run(args[0]);
    }
}
