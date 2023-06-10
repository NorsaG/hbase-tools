package org.evla.hbase.meta;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolRunner;
import org.evla.hbase.HBaseToolsHelper;
import org.evla.hbase.rstask.RSTaskControllerHelper;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MetaRunner implements HBaseToolRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaRunner.class);

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        TechnicalMeta technicalMeta = new TechnicalMeta(admin, settings);

        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Incorrect input parameters");
        }

        String parameter = args[0];

        long time = 0L;
        ServerName parsedServerName = null;

        if (args.length > 1) {
            time = Long.parseLong(args[1]);
        }

        if (args.length > 2) {
            parsedServerName = HBaseToolsHelper.selectServerName(admin, args[2]);
        }


        switch (parameter) {
            case "update_state": {
                ClusterTopology currentTopology = technicalMeta.getActualTopology(true);
                LOGGER.info("Actual topology size: {}\nServers:\n{}", currentTopology.getAllRegionsCount(), String.join("\n", currentTopology.getAllServers()));
                break;
            }
            case "fix_cluster": {
                LOGGER.info("Start restoring HBase Cluster");
                ClusterTopology currentTopology = technicalMeta.getActualTopology();
                LOGGER.info("Actual topology size: {}\nServers:\n{}", currentTopology.getAllRegionsCount(), String.join("\n", currentTopology.getAllServers()));
                ClusterTopology oldTopology = technicalMeta.getTopology(time);
                LOGGER.info("Topology size at {}: {}", time, oldTopology.getAllRegionsCount());

                try {
                    Collection<ServerName> servers = admin.getClusterStatus().getServers();
                    servers.forEach(serverName -> {
                        LOGGER.info("Start restoring Region Server: {}", serverName);
                        Difference diff = technicalMeta.getRegionsDiff(currentTopology, oldTopology, serverName);
                        diff.getDifference().forEach(r -> {
                            try {
                                if (r.getType() == DiffType.OUT) {
                                    String encodedRegionName = HRegionInfo.encodeRegionName(r.getRecord().getBytes(StandardCharsets.UTF_8));
                                    admin.move(encodedRegionName.getBytes(StandardCharsets.UTF_8), serverName);
                                    LOGGER.info("Move region {} to Region Server {}", encodedRegionName, serverName);
                                    RSTaskControllerHelper.sleep(250);
                                }
                            } catch (Exception e) {
                                LOGGER.error(e.getMessage(), e);
                            }
                        });
                        LOGGER.info("Restoring Region Server {} is completed", serverName);
                    });
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                LOGGER.info("Stop restoring HBase Cluster");
                break;
            }
            case "fix_server": {
                ClusterTopology currentTopology = technicalMeta.getActualTopology();
                LOGGER.info("Actual topology size: {}\nServers:\n{}", currentTopology.getAllRegionsCount(), String.join("\n", currentTopology.getAllServers()));
                LOGGER.info("Start restoring Region Server: {}", parsedServerName);
                ServerName serverName = parsedServerName;
                ClusterTopology oldTopology = technicalMeta.getTopology(time);
                LOGGER.info("Topology size at {}: {}\nServers:\n{}", time, oldTopology.getAllRegionsCount(), String.join("\n", oldTopology.getAllServers()));
                Difference diff = technicalMeta.getRegionsDiff(currentTopology, oldTopology, serverName);

                AtomicInteger counter = new AtomicInteger(0);
                diff.getDifference().forEach(r -> {
                    String encodedRegionName = HRegionInfo.encodeRegionName(r.getRecord().getBytes(StandardCharsets.UTF_8));
                    try {
                        if (r.getType() == DiffType.OUT) {
                            LOGGER.info("Trying to move region {} to Region Server {}", encodedRegionName, serverName);
                            admin.move(encodedRegionName.getBytes(StandardCharsets.UTF_8), serverName);
                            LOGGER.info("Region {} moved to Region Server {}", encodedRegionName, serverName);
                            counter.incrementAndGet();
                            RSTaskControllerHelper.sleep(250);
                        }
                    } catch (UnknownRegionException e) {
                        LOGGER.error("Unknown region: {}, {}", encodedRegionName, e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
                LOGGER.info("{} regions returned to {}", counter.get(), serverName);
                break;
            }
            case "diff": {
                ClusterTopology currentTopology = technicalMeta.getActualTopology();
                LOGGER.info("Actual topology size: {}\nServers:\n{}", currentTopology.getAllRegionsCount(), String.join("\n", currentTopology.getAllServers()));
                ClusterTopology oldTopology = technicalMeta.getTopology(time);
                LOGGER.info("Topology size at {}: {}\nServers:\n{}", time, oldTopology.getAllRegionsCount(), String.join("\n", oldTopology.getAllServers()));
                if (parsedServerName != null) {
                    LOGGER.info("Start analyze for {}", parsedServerName);
                    Difference diff = technicalMeta.getRegionsDiff(currentTopology, oldTopology, parsedServerName);
                    diff.getDifference().forEach(r -> LOGGER.info("Region information: {} => {}", r.getType(), r.getRecord()));
                } else {
                    Difference diff = technicalMeta.getServerDiff(currentTopology, oldTopology);
                    diff.getDifference().forEach(r -> LOGGER.info("RS information: {} => {}", r.getType(), r.getRecord()));
                }
                break;
            }

            default: {
                throw new RuntimeException("Incorrect input parameter");
            }
        }
    }

}
