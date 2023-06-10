package org.evla.hbase.report;

import java.util.List;

public interface SingleReport {

    void addReportedEvent(ReportedEvent event);

    List<ReportedEvent> getReportedEvents();

    Severity getReportSeverity();

    List<String> getReportInfo();

    String getReportName();

    Severity calculateReportSeverity();

    List<String> getHeaderForReport();
}
