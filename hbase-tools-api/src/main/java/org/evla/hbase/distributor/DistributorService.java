package org.evla.hbase.distributor;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.rstask.RSTaskControllerHelper;
import org.evla.hbase.compactor.LightweightCompactor;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.meta.MetaTableHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DistributorService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributorService.class);

    private final Admin admin;

    private final HBaseToolsSettings settings;
    private final int regionsWeightBorder;

    private final Set<TableName> processedTables = new LinkedHashSet<>();

    public DistributorService(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.settings = settings;
        this.regionsWeightBorder = settings.getDistributorSettings().getRegionWeightBorder();
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public void run() {
        LightweightCompactor compactor = new LightweightCompactor(admin, settings);
        Distributor distributor = new Distributor(admin, settings, compactor);
        MetaTableHolder holder = new MetaTableHolder();
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
            synchronized (processedTables) {
                LOGGER.info("Prepare to cleaning cached tables. Already processed {} tables", processedTables.size());
                processedTables.clear();
            }
        }, 6, 6, TimeUnit.HOURS);
        try {
            boolean needUpdateMetaCache = true;
            while (true) {
                try {
                    List<DistributeTableWeight> tableWeights = new ArrayList<>();
                    LOGGER.info("Start analyzing hbase:meta");
                    Map<TableName, Map<ServerName, List<String>>> tableDistributions = holder.getTablesDistribution(admin.getConnection(), admin.getClusterStatus().getServers(), needUpdateMetaCache);
                    needUpdateMetaCache = false;
                    for (TableName t : tableDistributions.keySet()) {
                        DistributeTableWeight weight = new DistributeTableWeight(t, tableDistributions.get(t));
                        if (weight.getTableWeight() > regionsWeightBorder && !processedTables.contains(weight.getTableName())) {
                            tableWeights.add(weight);
                        }
                    }
                    if (tableWeights.size() == 0) {
                        LOGGER.info("Waiting for next tasks.");
                        RSTaskControllerHelper.sleep(1_800_000);
                        needUpdateMetaCache = true;
                        continue;
                    }
                    tableWeights.sort(Collections.reverseOrder());
                    LOGGER.info("Start distributing tables.");
                    for (int i = 0; i < Math.min(tableWeights.size(), settings.getDistributorSettings().getRecalculateTableCount()); i++) {
                        DistributeTableWeight weight = tableWeights.get(i);
                        LOGGER.info("Table {}: {}", weight.getTableName(), weight.getTableWeight());
                        try {
                            boolean tableCompacted = distributor.runInChain(weight.getTableName());
                            processedTables.add(weight.getTableName());

                            if (tableCompacted) {
                                RSTaskControllerHelper.sleep(5_000);
                            }
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        } finally {
            compactor.stopCompact();
        }
    }
}
