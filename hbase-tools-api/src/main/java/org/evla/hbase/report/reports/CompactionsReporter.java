package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.evla.hbase.report.*;
import org.evla.hbase.Pair;
import org.evla.hbase.jmx.JMXRegionServerMetrics;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionsReporter implements Reporter {
    private final HBaseToolsSettings settings;

    public CompactionsReporter(HBaseToolsSettings settings) {
        this.settings = settings;
    }

    public Integer getJmxPort(Map<Integer, Integer> jmxPorts, int serverPort) {
        return jmxPorts.getOrDefault(serverPort, -1);
    }

    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SingleReport report = new CompactionSingleReport();
        AtomicInteger fullQueue = new AtomicInteger();

        Map<Integer, Integer> ports = settings.getCompactorSettings().getJmxPorts();
        clusterStatus.getServers().forEach(sn -> {
            JMXRegionServerMetrics jmx = new JMXRegionServerMetrics(sn, getJmxPort(ports, sn.getPort()));
            int compQueue = jmx.getCompactionQueueLength();

            report.addReportedEvent(new CompactionsReportedEvent(sn, compQueue, CompactionsReportedEvent.getCompactionEventSeverity(compQueue)));
            fullQueue.addAndGet(compQueue);
        });
        return report;
    }

    static class CompactionsReportedEvent implements ReportedEvent {
        private static final String COMPACTION_QUEUE = "COMPACTION QUEUE ON SERVER {0}: {1}";
        private static final String RESOLUTION_MESSAGE = "Wait until compactions stopped. Decrease cluster loads or analyze server {0}";

        private final ServerName serverName;
        private final int queueSize;
        private final Severity eventSeverity;

        CompactionsReportedEvent(ServerName serverName, int queueSize, Severity eventSeverity) {
            this.serverName = serverName;
            this.queueSize = queueSize;
            this.eventSeverity = eventSeverity;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.REGION_SERVER, new Pair<>(serverName, queueSize), MessageFormat.format(COMPACTION_QUEUE, serverName, queueSize), eventSeverity);
        }

        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.ADVICE, MessageFormat.format(RESOLUTION_MESSAGE, serverName));
        }

        public static Severity getCompactionEventSeverity(int compactionQueueSize) {
            return compactionQueueSize > 150 ? Severity.CRITICAL : (compactionQueueSize > 50 ? Severity.NORMAL : (compactionQueueSize > 15 ? Severity.LOW : Severity.NONE));
        }

        @Override
        public List<String> convertEvent() {
            return Arrays.asList(serverName.getServerName(), String.valueOf(queueSize), eventSeverity.toString());
        }
    }

    static class CompactionSingleReport extends DefaultSingleReport {
        @Override
        public String getReportName() {
            return "COMPACTION QUEUES";
        }

        @Override
        public Severity calculateReportSeverity() {
            int criticalCounts = 0;
            int normalCounts = 0;
            for (ReportedEvent re : getReportedEvents()) {
                CompactionsReportedEvent event = (CompactionsReportedEvent) re;
                Pair<ServerName, Integer> pair = (Pair<ServerName, Integer>) event.getSourceEvent().getEventObject();
                Severity severity = CompactionsReportedEvent.getCompactionEventSeverity(pair.getSecond());

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
        private static final List<String> columnNames = Arrays.asList("RegionServer", "Compaction queue", "Severity");

        @Override
        public List<String> getHeaderForReport() {
            return columnNames;
        }

    }
}
