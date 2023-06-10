package org.evla.hbase.common;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.evla.hbase.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

public class HBaseRegionChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRegionChecker.class);

    private final TableName tableName;

    public HBaseRegionChecker(TableName tn) {
        this.tableName = tn;
    }

    public Map<RegionInfo, Pair<Result, Result>> checkTable(Admin admin) throws Exception {
        List<RegionInfo> regions = admin.getRegions(tableName);
        AtomicReference<Exception> exc = new AtomicReference<>();

        Map<RegionInfo, Pair<Result, Result>> results = new TreeMap<>((h1, h2) -> Bytes.compareTo(h1.getStartKey(), h2.getStartKey()));
        regions.parallelStream().forEach(regionInfo -> {
            try {
                results.put(regionInfo, getBorderRecordsForRegion(admin, regionInfo));
            } catch (IOException e) {
                exc.set(e);
                LOGGER.error(e.getMessage(), e);
            }
        });
        if (exc.get() != null) {
            throw exc.get();
        }
        return results;
    }

    public Pair<Result, Result> getBorderRecordsForRegion(Admin admin, RegionInfo regionInfo) throws IOException {
        Scan scan = getScan(regionInfo, false);
        Result firstRecord = scanForFirstResult(admin, scan);

        Scan reversedScan = getScan(regionInfo, true);
        Result lastRecord = scanForFirstResult(admin, reversedScan);

        return new Pair<>(firstRecord, lastRecord);
    }

    private Result scanForFirstResult(Admin admin, Scan scan) throws IOException {
        Table table = admin.getConnection().getTable(tableName);
        try (ResultScanner resultScanner = table.getScanner(scan)) {
            return resultScanner.next();
        }
    }

    private Scan getScan(RegionInfo info, boolean reversed) {
        Scan scan = new Scan();
        scan.setMaxResultSize(1);
        scan.setCacheBlocks(false);
        if (reversed) {
            scan.setReversed(true);
        }
        scan.setStartRow(reversed ? info.getEndKey() : info.getStartKey());
        scan.setStopRow(reversed ? info.getStartKey() : info.getEndKey());
        return scan;
    }
}
