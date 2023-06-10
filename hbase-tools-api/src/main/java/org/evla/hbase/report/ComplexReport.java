package org.evla.hbase.report;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

interface ComplexReport {

    void addSingleReport(SingleReport report);

    List<SingleReport> getCompleteReports();

    default Severity getReportSeverity() {
        List<SingleReport> reports = getCompleteReports();
        int countCritical = 0, countNormal = 0, countLow = 0;

        for (SingleReport sr : reports) {
            if (sr.getReportSeverity() == Severity.CRITICAL) {
                countCritical++;
            } else if (sr.getReportSeverity() == Severity.NORMAL) {
                countNormal++;
            } else if (sr.getReportSeverity() == Severity.LOW) {
                countLow++;
            }
        }

        int weight = countCritical * 9 + countNormal * 3 + countLow;

        return weight > 10 ? Severity.CRITICAL : (weight > 7 ? Severity.NORMAL : (weight > 2 ? Severity.LOW : Severity.NONE));
    }


    String getReportTime();

    void makeResolutionFile();

     default void removeEmptyDestinations(Map<FixDestination, List<String>> resolution) {
        Set<FixDestination> emptySet = new HashSet<>();
        for (Map.Entry<FixDestination, List<String>> entry : resolution.entrySet()) {
            if (entry.getValue().size() == 0) {
                emptySet.add(entry.getKey());
            }
        }
        emptySet.forEach(resolution::remove);
    }
}
