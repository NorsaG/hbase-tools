package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.evla.hbase.report.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.Pair;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SnapshotsReport implements Reporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotsReport.class);
    private static final int DAYS_COUNT = 10;
    private static final int SNP_COUNT = 5;

    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SnapshotsSingleReport report = new SnapshotsSingleReport();
        try {
            final LocalDateTime ldt = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            List<SnapshotDescription> snapshots = admin.listSnapshots();
            report.setSnpCount(snapshots.size());

            Map<String, AtomicInteger> repeatedTables = new HashMap<>();
            snapshots.forEach(s -> {
                long creationTimeMillis = s.getCreationTime();
                Duration duration = Duration.between(LocalDateTime.ofInstant(Instant.ofEpochMilli(creationTimeMillis), ZoneId.systemDefault()), ldt);

                if (duration.toDays() >= DAYS_COUNT) {
                    LocalDateTime creationTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(creationTimeMillis), ZoneId.systemDefault());
                    report.addReportedEvent(new OldSnapshotReportedEvent(s.getTable(), s.getName(), formatter.format(creationTime)));
                }
                repeatedTables.putIfAbsent(s.getTable(), new AtomicInteger(0));
                repeatedTables.get(s.getTable()).incrementAndGet();

            });
            repeatedTables.entrySet().stream()
                    .filter(e -> e.getValue().get() >= SNP_COUNT)
                    .forEach(e -> report.addReportedEvent(new TooMuchSnapshotsReportedEvent(e.getKey(), e.getValue().get())));
        } catch (Exception e) {
            LOGGER.info(e.getMessage(), e);
        }

        return report;
    }

    private static class TooMuchSnapshotsReportedEvent implements ReportedEvent {

        private static final String PROBLEM_TEXT = "Too much snapshots";

        private final String tableName;
        private final int snpCount;

        TooMuchSnapshotsReportedEvent(String tableName, int snpCount) {
            this.tableName = tableName;
            this.snpCount = snpCount;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.SNAPSHOT, tableName + ": " + snpCount, PROBLEM_TEXT, Severity.NORMAL);
        }

        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.ADVICE, MessageFormat.format("Delete {1} snapshots for table: {0}", tableName, snpCount));
        }

        @Override
        public List<String> convertEvent() {
            return Collections.emptyList();
        }
    }

    private static class OldSnapshotReportedEvent implements ReportedEvent {
        private static final String PROBLEM_TEXT = "Too old snapshot";

        private final String tableName;
        private final String snpName;
        private final String created;

        OldSnapshotReportedEvent(String tableName, String snpName, String created) {
            this.tableName = tableName;
            this.snpName = snpName;
            this.created = created;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.SNAPSHOT, tableName + " => " + snpName, PROBLEM_TEXT, Severity.NORMAL);
        }

        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.HBASE_SHELL, MessageFormat.format("delete_snapshot ''{0}''", snpName));
        }

        @Override
        public List<String> convertEvent() {
            return Arrays.asList(tableName, snpName, created);
        }
    }

    private static class SnapshotsSingleReport extends DefaultSingleReport {
        private long snpCount = 0;

        @Override
        public String getReportName() {
            return "SNAPSHOTS";
        }

        public void setSnpCount(long count) {
            this.snpCount = count;
        }

        @Override
        public Severity calculateReportSeverity() {
            int events = getReportedEvents().size();
            return events > 100 ? Severity.CRITICAL :
                    (events > 25 ? Severity.NORMAL :
                            (events > 0 ? Severity.LOW : Severity.NONE)
                    );
        }

        @Override
        public List<String> getReportInfo() {
            return Arrays.asList(
                    MessageFormat.format("TOTAL SNAPSHOTS: {0}", snpCount),
                    MessageFormat.format("OLD SNAPSHOTS: {0}", getReportedEvents().size())
            );
        }

        @Override
        public List<String> getHeaderForReport() {
            return Arrays.asList("Table", "Snapshot Name", "Created");
        }
    }
}
