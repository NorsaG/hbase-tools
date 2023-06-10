package org.evla.hbase.distributor;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.compactor.LightweightCompactor;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.meta.MetaTableHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Distributor {
    private static final Logger LOGGER = LoggerFactory.getLogger(Distributor.class);

    private final Admin admin;

    private final HBaseToolsSettings settings;

    private final boolean needCompact;
    private final int threads;
    private final int regionsWeightBorder;

    private volatile LightweightCompactor compactor = null;

    public Distributor(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.settings = settings;
        this.regionsWeightBorder = settings.getDistributorSettings().getRegionWeightBorder();
        this.threads = settings.getDistributorSettings().getDistributorThreads();
        this.needCompact = settings.getDistributorSettings().isCompactAfterDistribute();
    }

    public Distributor(Admin admin, HBaseToolsSettings settings, LightweightCompactor compactor) {
        this(admin, settings);
        this.compactor = compactor;
    }

    public boolean runInChain(TableName table) {
        initCompactor();
        try {
            return runDistribution(admin.getClusterStatus(), table);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

    public void run(TableName table) {
        initCompactor();
        try {
            runDistribution(admin.getClusterStatus(), table);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        stopCompactor();
    }

    public void run(String tableMask) {
        initCompactor();
        runDistributions(tableMask);
        stopCompactor();
    }

    private void runDistributions(String tableMask) {
        try {
            System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(threads));
            TableName[] tables = admin.listTableNames(Pattern.compile(tableMask));
            LOGGER.info("Start distribution");
            ClusterStatus clusterStatus = admin.getClusterStatus();
            if (tables != null && tables.length < 11) {
                Arrays.stream(tables).parallel().forEach(t -> runDistribution(clusterStatus, t));
            } else if (tables != null) {
                List<DistributeTableWeight> tableWeights = new ArrayList<>();
                LOGGER.info("Start analyzing hbase:meta");
                Map<TableName, Map<ServerName, List<String>>> tableDistributions = new MetaTableHolder().getTablesDistribution(admin.getConnection(), admin.getClusterStatus().getServers());
                for (TableName t : tables) {
                    if (tableDistributions.get(t) == null) {
                        LOGGER.info("Distribution of {} is unknown", t);
                        continue;
                    }
                    DistributeTableWeight weight = new DistributeTableWeight(t, tableDistributions.get(t));
                    if (weight.getTableWeight() > regionsWeightBorder) {
                        tableWeights.add(weight);
                    }
                }
                tableWeights.sort(Collections.reverseOrder());
                LOGGER.info("There are {} tables that will be distributed. Average distributed weight is {}", tableWeights.size(), tableWeights.stream().map(DistributeTableWeight::getTableWeight).collect(Collectors.averagingDouble(Float::doubleValue)));

                tableWeights.parallelStream().forEach((weight) -> {
                    LOGGER.info("Table {}: {}", weight.getTableName(), weight.getTableWeight());
                    runDistribution(clusterStatus, weight.getTableName());
                });
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private synchronized void initCompactor() {
        if (needCompact && compactor == null) {
            compactor = new LightweightCompactor(admin, settings);
        }
    }

    private void stopCompactor() {
        if (compactor != null) {
            compactor.stopCompact();
            compactor = null;
        }
    }

    private boolean runDistribution(ClusterStatus clusterStatus, TableName table) {
        try {
            LOGGER.info("Start table distribution: {}", table);
            if (admin.isTableDisabled(table)) {
                LOGGER.warn("Table {} disabled. Skip it.", table);
                return false;
            }

            Collection<ServerName> servers = clusterStatus.getServers();
            List<ServerName> serversList = new ArrayList<>(servers);
            LOGGER.info("There are {} servers in cluster", serversList.size());

            TableDistributor distributor = new TableDistributor(admin, settings, compactor);
            distributor.distributeTable(serversList, table);
            LOGGER.info("Table {} distributed throughout the cluster", table);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

}
