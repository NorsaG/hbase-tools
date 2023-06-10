package org.evla.hbase.report;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class DefaultSingleReport implements SingleReport {
    private final List<ReportedEvent> reportedEvents = new ArrayList<>();
    private Severity severity = null;

    public DefaultSingleReport() {
    }

    @Override
    public abstract String getReportName();

    @Override
    public void addReportedEvent(ReportedEvent event) {
        this.reportedEvents.add(event);
    }

    @Override
    public List<ReportedEvent> getReportedEvents() {
        return reportedEvents;
    }

    @Override
    public Severity getReportSeverity() {
        if (severity == null) {
            severity = calculateReportSeverity();
        }
        return severity;
    }

    @Override
    public List<String> getReportInfo() {
        return Collections.emptyList();
    }
}
