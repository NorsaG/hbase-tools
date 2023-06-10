package org.evla.hbase.report.reports;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.compactor.CompactionWeight;
import org.evla.hbase.report.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.Pair;
import org.evla.hbase.meta.MetaTableHolder;

import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

public class BiggestRegionsReporter implements Reporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BiggestRegionsReporter.class);
    private MetaTableHolder holder;

    @Override
    public Reporter setMetaTableHolder(MetaTableHolder holder) {
        this.holder = holder;
        return this;
    }

    @Override
    public SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus) {
        SingleReport report = new BiggestRegionsSingleReport();
        makeSpecificCheck(admin, report, clusterStatus);
        return report;
    }

    private void makeSpecificCheck(Admin admin, SingleReport report, ClusterStatus clusterStatus) {
        List<HRegionInfo> regionInfos = new ArrayList<>();
        Map<String, RegionLoad> loads = new HashMap<>();
        for (ServerName sn : clusterStatus.getServers()) {
            regionInfos.addAll(getAllRegions(admin, sn, holder));
            loads.putAll(getRegionsLoad(clusterStatus, sn));
        }
        List<CompactionWeight> weights = getCompactionTasks(regionInfos, loads);

        if (weights.size() > 0) {
            weights.sort(Comparator.comparing(CompactionWeight::calculateRegionCompactionWeight));
            Collections.reverse(weights);
            for (CompactionWeight cw : weights) {
                float weight = cw.calculateRegionCompactionWeight();
                if (weight > 100) {
                    report.addReportedEvent(new BiggestRegionsReportedEvent(cw.getEncodedRegionName(), cw, Severity.CRITICAL));
                } else if (weight > 50) {
                    report.addReportedEvent(new BiggestRegionsReportedEvent(cw.getEncodedRegionName(), cw, Severity.NORMAL));
                } else if (weight > 30) {
                    report.addReportedEvent(new BiggestRegionsReportedEvent(cw.getEncodedRegionName(), cw, Severity.LOW));
                }
            }
        }
    }

    private List<CompactionWeight> getCompactionTasks(List<HRegionInfo> regionInfos, Map<String, RegionLoad> loads) {
        List<CompactionWeight> weights = new ArrayList<>();
        regionInfos.forEach(ri -> {
            RegionLoad rl = loads.get(ri.getEncodedName());
            if (rl == null) {
                LOGGER.debug("Empty RegionLoad. This is valid for moved region ({})", ri);
            } else {
                CompactionWeight weight = new CompactionWeight(rl);
                if (filterWeight(weight)) {
                    weights.add(weight);
                }
            }
        });

        return weights;
    }

    private boolean filterWeight(CompactionWeight weight) {
        return weight.calculateRegionCompactionWeight() > 15
                && weight.getTotalRegionSize() > 100;
    }

    private List<HRegionInfo> getAllRegions(Admin admin, ServerName sn, MetaTableHolder holder) {
        return new ArrayList<>(holder.getAllRegionsByServer(admin.getConnection(), sn, false));
    }

    private Map<String, RegionLoad> getRegionsLoad(ClusterStatus status, ServerName sn) {
        try {
            ServerLoad sl = status.getLoad(sn);
            return sl == null ? Collections.emptyMap() : sl.getRegionsLoad()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(k -> HRegionInfo.encodeRegionName(k.getValue().getName()), Map.Entry::getValue));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw new RuntimeException("Cant create CompactorManager");
        }
    }

    static class BiggestRegionsReportedEvent implements ReportedEvent {
        private static final String BAD_COMPACTION = "Region should be compacted";

        private final String encodedRegionName;
        private final CompactionWeight weight;
        private final Severity eventSeverity;

        BiggestRegionsReportedEvent(String encodedRegionName, CompactionWeight weight, Severity eventSeverity) {
            this.encodedRegionName = HRegionInfo.encodeRegionName(encodedRegionName.getBytes(StandardCharsets.UTF_8));
            this.weight = weight;
            this.eventSeverity = eventSeverity;
        }

        @Override
        public SingleEvent getSourceEvent() {
            return new SingleEvent(EventType.REGION, encodedRegionName, BAD_COMPACTION + weight, eventSeverity);
        }

        @Override
        public Pair<FixDestination, String> resolution() {
            return new Pair<>(FixDestination.HBASE_SHELL, MessageFormat.format("major_compact ''{0}''", encodedRegionName));
        }


        @Override
        public List<String> convertEvent() {
            return Arrays.asList(
                    HRegionInfo.getTable(weight.getEncodedRegionName().getBytes(StandardCharsets.UTF_8)).toString(),
                    encodedRegionName,
                    weight.getLocality().toString(),
                    String.valueOf(weight.getStoreFileCount()),
                    String.valueOf(weight.getTotalRegionSize()),
                    weight.calculateRegionCompactionWeight().toString(),
                    eventSeverity.toString());
        }
    }

    static class BiggestRegionsSingleReport extends DefaultSingleReport {
        @Override
        public String getReportName() {
            return "REGIONS FOR COMPACTION";
        }

        @Override
        public Severity calculateReportSeverity() {
            int criticalCounts = 0;
            int normalCounts = 0;
            for (ReportedEvent re : getReportedEvents()) {
                BiggestRegionsReportedEvent reportedEvent = (BiggestRegionsReportedEvent) re;
                SingleEvent singleEvent = reportedEvent.getSourceEvent();
                Severity severity = singleEvent.getSeverity();

                if (severity == Severity.CRITICAL) {
                    criticalCounts++;
                } else if (severity == Severity.NORMAL) {
                    normalCounts++;
                }
            }
            int weight = criticalCounts * 10 + normalCounts;
            return (weight > 500 ? Severity.CRITICAL : (weight > 200 ? Severity.NORMAL : (weight > 20 ? Severity.LOW : Severity.NONE)));
        }

        private static final List<String> columnNames = Arrays.asList(
                "Table",
                "Region Encoded Name",
                "Locality",
                "Store File`s Count",
                "Total Region Size",
                "Calculated Weight",
                "Severity");

        @Override
        public List<String> getHeaderForReport() {
            return columnNames;
        }

    }
}
