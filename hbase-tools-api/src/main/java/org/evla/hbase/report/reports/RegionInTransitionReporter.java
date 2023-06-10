package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState;
import org.evla.hbase.report.*;
import org.evla.hbase.Pair;
import org.evla.hbase.rstask.RSTaskControllerHelper;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class RegionInTransitionReporter implements Reporter {

    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SingleReport report = new RegionInTransitionSingleReport();
        LocalDateTime ldt = LocalDateTime.now();
        Map<String, RegionState> rit = RSTaskControllerHelper.getRegionsInTransitionMap(clusterStatus);
        rit.forEach((key, value) -> {
            long start = value.getStamp();
            Instant i = Instant.ofEpochMilli(start);
            Duration d = Duration.between(LocalDateTime.ofInstant(i, ZoneId.systemDefault()), ldt);

            if (d.toMinutes() > 1) {
                RegionInfo info = value.getRegion();
                report.addReportedEvent(new LongTransitionEvent(info.getTable().toString(), info.getEncodedName()));
            }
        });
        return report;
    }

    static class LongTransitionEvent implements ReportedEvent {

        private static final String PROBLEM_TEXT = "Region transition takes too much time";

        private final String table;
        private final String region;

        LongTransitionEvent(String table, String region) {
            this.table = table;
            this.region = region;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.REGION, table + " => " + region, PROBLEM_TEXT, Severity.NORMAL);
        }

        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.HBASE_SHELL, MessageFormat.format("unassign ''{0}''", region) + "\n" + MessageFormat.format("assign ''{0}''", region));
        }

        @Override
        public List<String> convertEvent() {
            return Arrays.asList(table, region, PROBLEM_TEXT);
        }
    }

    static class RegionInTransitionSingleReport extends DefaultSingleReport {
        @Override
        public String getReportName() {
            return "REGIONS IN TRANSITION";
        }

        @Override
        public Severity calculateReportSeverity() {
            int reportedEvents = getReportedEvents().size();
            return reportedEvents > 15 ? Severity.CRITICAL : (
                    reportedEvents > 5 ? Severity.NORMAL : (
                            reportedEvents > 0 ? Severity.LOW : Severity.NONE
                    )
            );
        }

        @Override
        public List<String> getHeaderForReport() {
            return Arrays.asList("Table", "Region", "Problem");
        }

    }
}
