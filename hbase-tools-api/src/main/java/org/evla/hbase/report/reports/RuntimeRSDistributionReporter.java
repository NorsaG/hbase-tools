package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.report.DefaultSingleReport;
import org.evla.hbase.report.Reporter;
import org.evla.hbase.report.Severity;
import org.evla.hbase.report.SingleReport;
import org.evla.hbase.Pair;

import java.util.*;

public class RuntimeRSDistributionReporter implements Reporter {

    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SingleReport report = new RuntimeRSDistributionReport();

        List<Pair<ServerName, Integer>> countsList = new ArrayList<>();
        final int regionsCount = (int) clusterStatus.getAverageLoad();
        clusterStatus.getServers().forEach(sn -> {
            Map<byte[], RegionLoad> loads = clusterStatus.getLoad(sn).getRegionsLoad();
            countsList.add(new Pair<>(sn, loads.size()));
        });
        countsList.forEach(p -> report.addReportedEvent(new MetaRSDistributionReporter.MetaRSDistributionEvent(
                p.getFirst(), p.getSecond(), getSeverity(p, regionsCount), getSide(p, regionsCount))
        ));

        countsList.sort(Comparator.comparingInt(Pair::getSecond));
        return report;
    }

    private MetaRSDistributionReporter.Side getSide(Pair<ServerName, Integer> p, int regionsCount) {
        return (p.getSecond() > regionsCount * 1.2) ? MetaRSDistributionReporter.Side.MORE_THEN_AVG : ((p.getSecond() < regionsCount * 1.2) ? MetaRSDistributionReporter.Side.LESS_THAN_AVG : MetaRSDistributionReporter.Side.MIDDLE);
    }

    private Severity getSeverity(Pair<ServerName, Integer> p, int regionsCount) {
        return p.getSecond() > (regionsCount * 2) ? Severity.NORMAL : (
                p.getSecond() > (int) (regionsCount * 1.3) || p.getSecond() < regionsCount * 0.3 ? Severity.LOW : Severity.NONE
        );
    }

    static class RuntimeRSDistributionReport extends DefaultSingleReport {
        @Override
        public String getReportName() {
            return "REGION`S DISTRIBUTION (MEMORY)";
        }

        @Override
        public Severity calculateReportSeverity() {
            return Severity.NONE;
        }

        @Override
        public List<String> getHeaderForReport() {
            return Arrays.asList(
                    "Region Server",
                    "Region`s Count",
                    "Severity",
                    "Problem");
        }

    }
}
