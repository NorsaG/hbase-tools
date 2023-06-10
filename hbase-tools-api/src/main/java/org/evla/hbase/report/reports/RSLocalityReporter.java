package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.report.*;
import org.evla.hbase.Pair;
import org.evla.hbase.jmx.JMXRegionServerMetrics;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;


public class RSLocalityReporter implements Reporter {
    private final HBaseToolsSettings settings;

    public RSLocalityReporter(HBaseToolsSettings settings) {
        this.settings = settings;
    }

    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SingleReport report = new RSLocalityReport();

        clusterStatus.getServers().forEach(sn -> {
            JMXRegionServerMetrics jmx = new JMXRegionServerMetrics(sn, settings.getCompactorSettings().getJmxPort(sn.getPort()));
            double locality = jmx.getPercentFilesLocal();
            locality = Double.parseDouble(String.format("%.2f", locality));
            report.addReportedEvent(new RSLocalityReportedEvent(sn, locality, RSLocalityReport.getCompactionEventSeverity(locality)));
        });
        return report;
    }

    static class RSLocalityReport extends DefaultSingleReport {
        @Override
        public String getReportName() {
            return "REGION SERVER LOCALITY REPORT";
        }

        @Override
        public Severity calculateReportSeverity() {
            int criticalCounts = 0;
            int normalCounts = 0;
            for (ReportedEvent re : getReportedEvents()) {
                RSLocalityReportedEvent event = (RSLocalityReportedEvent) re;

                Pair<ServerName, Double> pair = (Pair<ServerName, Double>) event.getSourceEvent().getEventObject();
                Severity severity = getCompactionEventSeverity(pair.getSecond());

                if (severity == Severity.CRITICAL) {
                    criticalCounts++;
                } else if (severity == Severity.NORMAL) {
                    normalCounts++;
                }
            }
            int weight = criticalCounts * 10 + normalCounts;
            return (weight > 19 || weight > 15 && criticalCounts > 0) ? Severity.CRITICAL :
                    (weight > 5 ? Severity.NORMAL :
                            (weight > 0 ? Severity.LOW : Severity.NONE));
        }

        private static final List<String> columnNames = Arrays.asList("RegionServer", "Locality", "Severity");

        @Override
        public List<String> getHeaderForReport() {
            return columnNames;
        }

        public static Severity getCompactionEventSeverity(double locality) {
            return locality < 65 ? Severity.CRITICAL : (locality < 85 ? Severity.NORMAL : (locality < 95 ? Severity.LOW : Severity.NONE));
        }
    }

    static class RSLocalityReportedEvent implements ReportedEvent {
        private static final String LOCALITY_MESSAGE = "LOW LOCALITY ON {0}: {1}";
        private static final String RESOLUTION_MESSAGE = "Run compactor on cluster or for {0} Region Server";

        private final ServerName serverName;
        private final double locality;
        private final Severity eventSeverity;

        RSLocalityReportedEvent(ServerName serverName, double locality, Severity eventSeverity) {
            this.serverName = serverName;
            this.locality = locality;
            this.eventSeverity = eventSeverity;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.REGION_SERVER, new Pair<>(serverName, locality), MessageFormat.format(LOCALITY_MESSAGE, serverName, locality), eventSeverity);
        }

        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.ADVICE, MessageFormat.format(RESOLUTION_MESSAGE, serverName));
        }

        @Override
        public List<String> convertEvent() {
            return Arrays.asList(serverName.getServerName(), String.valueOf(locality), eventSeverity.toString());
        }
    }
}
