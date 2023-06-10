package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.report.*;
import org.evla.hbase.Pair;
import org.evla.hbase.meta.MetaTableHolder;

import java.util.*;
import java.util.stream.Collectors;

public class MetaRSDistributionReporter implements Reporter {
    private MetaTableHolder holder;

    @Override
    public MetaRSDistributionReporter setMetaTableHolder(MetaTableHolder holder) {
        this.holder = holder;
        return this;
    }

    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SingleReport report = new MetaRSSingleReport();
        Map<ServerName, Set<HRegionInfo>> allInfo = holder.getAllRegions(clusterStatus.getServers(), admin.getConnection());

        Integer serversCount = clusterStatus.getServersSize();
        Integer regionsCount = allInfo.values().stream().mapToInt(Set::size).sum();

        final int avgRegionsCount = regionsCount / serversCount;
        allInfo.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()))
                .entrySet()
                .stream()
                .map(e -> new Pair<>(e.getKey(), e.getValue()))
                .sorted(Comparator.comparingInt(Pair::getSecond))
                .forEach(p -> report.addReportedEvent(new MetaRSDistributionEvent(p.getFirst(), p.getSecond(),
                        p.getSecond() > (avgRegionsCount * 2) ? Severity.NORMAL : (
                                p.getSecond() > (int) (avgRegionsCount * 1.3) || p.getSecond() < avgRegionsCount * 0.3 ? Severity.LOW : Severity.NONE
                        ), p.getSecond() > avgRegionsCount ? Side.MORE_THEN_AVG : Side.LESS_THAN_AVG
                )));


        return report;
    }

    public static class MetaRSDistributionEvent implements ReportedEvent {
        private static final String RESOLUTION_TEXT = "Check or monitor RS status. Run distributor for bad-distributed tables or namespaces.";

        private final ServerName sn;
        private final Integer regionsCount;
        private final Severity severity;
        private final Side side;


        MetaRSDistributionEvent(ServerName sn, Integer regionsCount, Severity severity, Side side) {
            this.sn = sn;
            this.regionsCount = regionsCount;
            this.severity = severity;
            this.side = side;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.CLUSTER, sn.toString() + ": " + regionsCount, side.getText(), severity);
        }

        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.NONE, RESOLUTION_TEXT);
        }

        @Override
        public List<String> convertEvent() {
            return Arrays.asList(sn.getServerName(), String.valueOf(regionsCount), severity.toString(), side.toString());
        }
    }

    enum Side {
        MIDDLE {
            @Override
            public String getText() {
                return "";
            }
        },
        MORE_THEN_AVG {
            @Override
            public String getText() {
                return "RS has too much regions";
            }
        }, LESS_THAN_AVG {
            @Override
            public String getText() {
                return "RS has small numbers of regions";
            }
        };

        public abstract String getText();
    }

    static class MetaRSSingleReport extends DefaultSingleReport {
        @Override
        public String getReportName() {
            return "REGION`S DISTRIBUTION (META)";
        }

        @Override
        public Severity calculateReportSeverity() {
            return Severity.NONE;
        }

        private static final List<String> columnNames = Arrays.asList(
                "Region Server",
                "Region`s Count",
                "Severity",
                "Problem");

        @Override
        public List<String> getHeaderForReport() {
            return columnNames;
        }

    }
}
