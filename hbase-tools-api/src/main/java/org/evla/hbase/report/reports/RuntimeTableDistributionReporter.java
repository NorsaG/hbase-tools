package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.report.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.Pair;
import org.evla.hbase.distributor.DistributeTableWeight;
import org.evla.hbase.meta.MetaTableHolder;

import java.text.MessageFormat;
import java.util.*;

public class RuntimeTableDistributionReporter implements Reporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(RuntimeTableDistributionReporter.class);

    private MetaTableHolder holder;

    @Override
    public Reporter setMetaTableHolder(MetaTableHolder holder) {
        this.holder = holder;
        return this;
    }

    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SingleReport report = new DefaultSingleReport() {
            @Override
            public String getReportName() {
                return "TABLE REGIONS DISTRIBUTION THROUGHOUT THE CLUSTER";
            }

            @Override
            public Severity calculateReportSeverity() {
                //todo!!!!!!!!!!!!!!!!
                return Severity.NONE;
            }

            @Override
            public List<String> getHeaderForReport() {
                return Arrays.asList("Table", "Moves", "Region`s Count", "Severity");
            }
        };
        addInfoToReport(admin, clusterStatus, report);

        return report;
    }

    private void addInfoToReport(Admin admin, ClusterStatus clusterStatus, SingleReport report) {
        try {
            TableName[] allTables = admin.listTableNames();
            List<DistributeTableWeight> tableWeights = new ArrayList<>();
            Map<TableName, Map<ServerName, List<String>>> tableDistributions = holder.getTablesDistribution(admin.getConnection(), admin.getClusterStatus().getServers());
            for (TableName t : allTables) {
                if (tableDistributions.get(t) == null) {
                    LOGGER.debug("Distribution of {} is unknown", t);
                    continue;
                }
                DistributeTableWeight weight = new DistributeTableWeight(t, tableDistributions.get(t));
                if (weight.getTableWeight() > 1) {
                    tableWeights.add(weight);
                }
            }
            tableWeights.sort(DistributeTableWeight::compareTo);
            Collections.reverse(tableWeights);

            tableWeights.forEach(w -> report.addReportedEvent(new TableDistributionReportedEvent(w.getTableName().toString(), w)));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    static class TableDistributionReportedEvent implements ReportedEvent {
        private static final String DISTRIBUTOR_RUN_SCRIPT = "./runner.sh distributor {0}";

        private final String tableName;
        private final DistributeTableWeight weight;

        TableDistributionReportedEvent(String tableName, DistributeTableWeight weight) {
            this.tableName = tableName;
            this.weight = weight;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.TABLE, tableName, "Bad distribution: " + weight, calculateSeverity(weight));
        }


        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.SHELL, MessageFormat.format(DISTRIBUTOR_RUN_SCRIPT, tableName));
        }

        @Override
        public List<String> convertEvent() {
            return Arrays.asList(tableName, String.valueOf(weight.getTableWeight()), String.valueOf(weight.getRegionsCount()), getSourceEvent().getSeverity().toString());
        }
    }

    public static Severity calculateSeverity(DistributeTableWeight weight) {
        int moves = weight.getTableWeight().intValue();
        int tableRegions = weight.getRegionsCount();

        double calcPercent = ((double) moves) / tableRegions;
        if (weight.getRegionsCount() > 100) {
            if (calcPercent > 0.15)
                return Severity.CRITICAL;
            else if (calcPercent > 0.05)
                return Severity.NORMAL;
            else
                return Severity.LOW;
        } else {
            if (calcPercent > 0.25)
                return Severity.CRITICAL;
            else if (calcPercent > 0.1)
                return Severity.NORMAL;
            else
                return Severity.LOW;
        }
    }

}
