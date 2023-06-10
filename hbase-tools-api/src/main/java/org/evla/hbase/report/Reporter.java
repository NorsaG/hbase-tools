package org.evla.hbase.report;

import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.meta.MetaTableHolder;

public interface Reporter {

    default Reporter setMetaTableHolder(MetaTableHolder holder) {
        return this;
    }

    SingleReport runReporterAndCreateReport(Admin admin, ClusterStatus clusterStatus);

    default SingleReport runReporterAndSetSeverity(Admin admin, ClusterStatus clusterStatus) {
        SingleReport singleReport = runReporterAndCreateReport(admin, clusterStatus);
        singleReport.getReportSeverity();
        return singleReport;
    }
}
