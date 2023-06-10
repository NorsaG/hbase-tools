package org.evla.hbase.report;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.meta.MetaTableHolder;
import org.evla.hbase.report.reports.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.HBaseToolRunner;


public class ReportRunner implements HBaseToolRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportRunner.class);

    @Override
    public void run(Admin admin, HBaseToolsSettings settings, String... args) {
        makeReport(admin, settings, args);
    }

    private void makeReport(Admin admin, HBaseToolsSettings settings, String[] args) {
        try {
            ClusterStatus status = admin.getClusterStatus();
            MetaTableHolder holder = new MetaTableHolder();
            holder.getAllRegions(status.getServers(), admin.getConnection());
            ComplexReport cr;
            if (args == null || args.length == 0 || "partial".equals(args[0])) {
                cr = new PartialReport();
            } else if ("full".equals(args[0])) {
                cr = new FullReport();
            } else {
                throw new RuntimeException("Unknown report type");
            }
            ReportBuilder reportBuilder = new ReportBuilder(admin, status, holder, cr);
            reportBuilder.addReporter(new BiggestRegionsReporter());
            reportBuilder.addReporter(new CompactionsReporter(settings));
            reportBuilder.addReporter(new MetaRSDistributionReporter());
            reportBuilder.addReporter(new RegionInTransitionReporter());
            reportBuilder.addReporter(new RuntimeRSDistributionReporter());
            reportBuilder.addReporter(new RuntimeTableDistributionReporter());
            reportBuilder.addReporter(new SnapshotsReport());
            reportBuilder.addReporter(new RegionHeapSizeReporter());
            reportBuilder.addReporter(new RSLocalityReporter(settings));
            reportBuilder.makeReport(ReportBuilder.ReportMode.HTML);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }
}
