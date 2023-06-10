package org.evla.hbase.report;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.evla.hbase.meta.MetaTableHolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ReportBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReportBuilder.class);

    private final List<Reporter> reporters = new ArrayList<>();

    private final Admin admin;
    private final ClusterStatus clusterStatus;
    private final MetaTableHolder holder;
    private final ComplexReport report;

    public ReportBuilder(Admin admin, ClusterStatus clusterStatus, MetaTableHolder holder, ComplexReport report) {
        this.admin = admin;
        this.clusterStatus = clusterStatus;
        this.holder = holder;
        this.report = report;
    }

    public ReportBuilder addReporter(Reporter reporter) {
        reporter.setMetaTableHolder(holder);
        this.reporters.add(reporter);
        return this;
    }

    public ReportBuilder addReporters(List<Reporter> reporters) {
        reporters.forEach(r -> r.setMetaTableHolder(holder));
        this.reporters.addAll(reporters);
        return this;
    }

    public void makeReport(ReportMode mode) {
        mode.makeReport(report, reporters, admin, clusterStatus);
    }

    enum ReportMode {
        CONSOLE {
            @Override
            public void doReport(ComplexReport cr, List<Reporter> reporters, Admin admin, ClusterStatus clusterStatus) {
                for (Reporter r : reporters) {
                    SingleReport report = r.runReporterAndSetSeverity(admin, clusterStatus);
                    cr.addSingleReport(report);
                }
                for (String s : getReportHeader(admin, cr)) {
                    System.out.println(s);
                }
                for (SingleReport report : cr.getCompleteReports()) {
                    System.out.println("REPORT: " + report.getReportName());
                    System.out.println("REPORT STATE: " + report.getReportSeverity());
                    for (String s : report.getReportInfo()) {
                        System.out.println(s);
                    }
                    for (ReportedEvent reportedEvent : report.getReportedEvents()) {
                        if (reportedEvent.getSourceEvent().getSeverity() == Severity.NONE) {
                            SingleEvent singleEvent = reportedEvent.getSourceEvent();
                            System.out.println(singleEvent.getType() + ", " + singleEvent.getEventObject());
                        } else {
                            SingleEvent singleEvent = reportedEvent.getSourceEvent();
                            System.out.println(singleEvent.getSeverity() + ": " + singleEvent.getType() + ", " + singleEvent.getEventObject() + " => " + singleEvent.getProblem() + " :: " + reportedEvent.resolution());
                        }

                    }
                }
            }

        }, LOG {
            @Override
            public void doReport(ComplexReport cr, List<Reporter> reporters, Admin admin, ClusterStatus clusterStatus) {

            }


        }, HTML {
            private void appendStyle(StringBuilder sb) {
                sb
                        .append("<style>")
                        .append("table, th, td {border: 1px solid black}")
                        .append("</style>");
            }

            @Override
            public void doReport(ComplexReport cr, List<Reporter> reporters, Admin admin, ClusterStatus clusterStatus) {
                StringBuilder sb = new StringBuilder();
                for (Reporter r : reporters) {
                    SingleReport report = r.runReporterAndSetSeverity(admin, clusterStatus);
                    cr.addSingleReport(report);
                }
                sb.append("<html>");
                appendStyle(sb);
                sb.append("<body>");

                for (String s : getReportHeader(admin, cr)) {
                    sb.append("<h1>").append(s).append("</h1>");
                }
                for (SingleReport report : cr.getCompleteReports()) {
                    sb.append("<h3>REPORT: ").append(report.getReportName()).append("</h3>");
                    sb.append("<h3>REPORT SEVERITY: ").append(report.getReportSeverity()).append("</h3>");
                    for (String s : report.getReportInfo()) {
                        sb.append("<h5>").append(s).append("</h5>");
                    }
                    if (report.getReportedEvents().size() > 0) {
                        sb.append("<table style=\"border: 1px solid black\">");
                        sb.append("<tr>");
                        for (String s : report.getHeaderForReport()) {
                            sb.append("<th>").append(s).append("</th>");
                        }
                        sb.append("</tr>");
                        for (ReportedEvent reportedEvent : report.getReportedEvents()) {
                            sb.append("<tr>");
                            for (String s : reportedEvent.convertEvent()) {
                                sb.append("<td>").append(s).append("</td>");
                            }
                            sb.append("</tr>");
                        }
                        sb.append("</table>");
                    }
                }
                sb.append("</body></html>");

                try {
                    Path p = Paths.get("report-" + cr.getReportTime().replaceAll(" ", "_").replaceAll(":", "-") + ".html");
                    Files.write(p, sb.toString().getBytes(StandardCharsets.UTF_8));
                    LOGGER.info("Report is ready: {}", p.toAbsolutePath());
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }


        };

        public void makeReport(ComplexReport cr, List<Reporter> reporters, Admin admin, ClusterStatus clusterStatus) {
            doReport(cr, reporters, admin, clusterStatus);
            cr.makeResolutionFile();
        }



        public abstract void doReport(ComplexReport cr, List<Reporter> reporters, Admin admin, ClusterStatus clusterStatus);

        public List<String> getReportHeader(Admin admin, ComplexReport cr) {
            String cluster = admin.getConfiguration().get("fs.defaultFS").replace("hdfs://", "");
            String time = cr.getReportTime();
            return Arrays.asList("REPORT TIME: " + time,
                    "CLUSTER: " + cluster,
                    "REPORT SEVERITY: " + cr.getReportSeverity());
        }
    }
}
