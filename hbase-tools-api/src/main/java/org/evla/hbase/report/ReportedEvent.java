package org.evla.hbase.report;

import org.evla.hbase.Pair;

import java.util.List;

public interface ReportedEvent {

    SingleEvent getSourceEvent();

    Pair<FixDestination, String> resolution();

    List<String> convertEvent();
}
