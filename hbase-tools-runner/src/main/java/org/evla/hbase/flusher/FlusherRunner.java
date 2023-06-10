package org.evla.hbase.flusher;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.configuration.HBaseToolsSettings;

public class FlusherRunner implements HBaseToolRunner {

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        Flusher flusher = new Flusher(admin, settings);
        if (args != null) {
            flusher.start(args[0]);
        } else {
            flusher.start();
        }
    }
}
