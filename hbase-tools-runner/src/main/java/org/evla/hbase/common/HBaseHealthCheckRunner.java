package org.evla.hbase.common;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.analyze.HBaseHealthAnalyzeService;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.meta.MetaTableHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseHealthCheckRunner implements HBaseToolRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseHealthCheckRunner.class);

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        HBaseHealthAnalyzeService service = new HBaseHealthAnalyzeService(admin, settings, new MetaTableHolder(), true);
        LOGGER.info("Status: \n{}",service.getClusterAvailabilityObject());
    }
}
