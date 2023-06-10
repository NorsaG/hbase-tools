package org.evla.hbase.report;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PartialReport implements ComplexReport {
    private static final Logger LOGGER = LoggerFactory.getLogger(PartialReport.class);
    private static final int PARTIAL_NUMBER_OF_EVENTS = 15;

    private final List<SingleReport> reports = new ArrayList<>();
    private String reportTime = null;

    @Override
    public void addSingleReport(SingleReport report) {
        List<ReportedEvent> reportedEvents = report.getReportedEvents();
//        reportedEvents.sort(Comparator.comparing(o -> o.getSourceEvent().getSeverity()));
        if (reportedEvents.size() > PARTIAL_NUMBER_OF_EVENTS) {
            reportedEvents.subList(PARTIAL_NUMBER_OF_EVENTS, reportedEvents.size()).clear();
        }
        this.reports.add(report);
    }

    @Override
    public List<SingleReport> getCompleteReports() {
        return this.reports;
    }

    @Override
    public String getReportTime() {
        if (reportTime == null) {
            reportTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
        return reportTime;
    }

    public void makeResolutionFile() {
        Map<FixDestination, List<String>> resolution = new HashMap<>();
        for (SingleReport sr : getCompleteReports()) {
            for (ReportedEvent re : sr.getReportedEvents()) {
                resolution.putIfAbsent(re.resolution().getFirst(), new ArrayList<>());
                if (re.getSourceEvent().getSeverity() != Severity.NONE) {
                    resolution.get(re.resolution().getFirst()).add(re.resolution().getSecond());
                }
            }
        }
        removeEmptyDestinations(resolution);
        StringJoiner sj = new StringJoiner("\n");
        for (FixDestination destination : resolution.keySet()) {

            for (String s : resolution.get(destination)) {
                if (destination == FixDestination.HBASE_SHELL) {
                    String str = "echo \"" + s.replaceAll("'", "\\'") + "\" | hbase shell ";
                    sj.add(str);
                } else if (destination == FixDestination.SHELL) {
                    sj.add(s);
                }
            }
            sj.add("\n");
        }
        try {
            Path file = Paths.get("fix-cluster-" + getReportTime().replaceAll(" ", "_").replaceAll(":", "-") + ".sh");
            Files.write(file, sj.toString().getBytes(StandardCharsets.UTF_8));
            LOGGER.info("File with fixes is ready: {}", file.toAbsolutePath());
        } catch (Exception e) {
            LOGGER.error("Cant create resolution file.", e);
        }

    }
}
