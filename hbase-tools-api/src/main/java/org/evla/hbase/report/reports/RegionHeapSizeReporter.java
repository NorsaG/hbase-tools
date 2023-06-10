package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.report.*;
import org.evla.hbase.Pair;

import java.text.MessageFormat;
import java.util.*;

public class RegionHeapSizeReporter implements Reporter {
    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SingleReport report = new RegionHeapSizeReport();

        Collection<ServerName> servers = clusterStatus.getServers();
        Map<ServerName, ServerLoad> serverLoads = new HashMap<>();
        for (ServerName sn : servers) {
            ServerLoad sl = clusterStatus.getLoad(sn);
            serverLoads.put(sn, sl);
        }

        serverLoads.entrySet().stream().filter(e -> {
            ServerLoad sl = e.getValue();
            return ((float) sl.getUsedHeapMB() / (float) sl.getMaxHeapMB()) > 0.8f;
        }).forEach(e -> report.addReportedEvent(new RegionHeapSizeReportedEvent(e.getKey(), e.getValue())));

        return report;
    }

    static class RegionHeapSizeReportedEvent implements ReportedEvent {
        private static final String ADVICE_MESSAGE = "Check RS state. In some cases you can flush regions or move biggest regions to another RS.";
        private final ServerName name;
        private final ServerLoad load;

        RegionHeapSizeReportedEvent(ServerName name, ServerLoad load) {
            this.name = name;
            this.load = load;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.REGION_SERVER, name, MessageFormat.format("Heap size of {0} use {1}%", name.getHostAndPort(), load.getUsedHeapMB() * 100 / load.getMaxHeapMB()), Severity.LOW);
        }

        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.ADVICE, ADVICE_MESSAGE);
        }

        @Override
        public List<String> convertEvent() {
            return Arrays.asList(name.getServerName(), String.valueOf(load.getUsedHeapMB()), (float) load.getUsedHeapMB() * 100  / (float) load.getMaxHeapMB() + "%");
        }
    }

    static class RegionHeapSizeReport extends DefaultSingleReport {
        @Override
        public String getReportName() {
            return "REGION SERVER HEAP SIZE";
        }

        @Override
        public Severity calculateReportSeverity() {
            List<ReportedEvent> reports = getReportedEvents();
            return reports.size() > 25 ? Severity.CRITICAL : (reports.size() > 10 ? Severity.NORMAL : (reports.size() > 0 ? Severity.LOW : Severity.NONE));
        }

        @Override
        public List<String> getHeaderForReport() {
            return Arrays.asList("Region Server", "Current Heap Size (Mb)", "Percent");
        }
    }

}
