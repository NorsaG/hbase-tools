package org.evla.hbase.common;

import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class HBaseCompactionQueueCleaner {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRegionChecker.class);

    public HBaseCompactionQueueCleaner() {
    }

    public void cleanQueues(Admin admin) {
        LOGGER.info("Start cleanup cluster");
        try {
            admin.getRegionServers().forEach(serverName -> {
                try {
                    admin.clearCompactionQueues(serverName, new HashSet<>(Arrays.asList("long", "short")));
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("Cant call cleaning of Compaction Queues for RS: {}. {}", serverName, e.getMessage());
                }
            });
        } catch (IOException e) {
            LOGGER.error("Cant call cleaning of Compaction Queues", e);
        }
        LOGGER.info("Cleanup is done");
    }

}