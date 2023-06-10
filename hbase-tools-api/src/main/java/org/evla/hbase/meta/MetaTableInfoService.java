package org.evla.hbase.meta;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class MetaTableInfoService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MetaTableInfoService.class);

    public static Set<HRegionInfo> getAllTableRegions(Connection connection) {
        return getTableRegions(connection, null);
    }

    public static Set<HRegionInfo> getTableRegions(Connection connection, ServerName serverName) {
        CollectingVisitor<HRegionInfo> visitor = new CollectingVisitor<HRegionInfo>(serverName) {
            private RegionLocations current = null;

            @Override
            public boolean visit(Result r) throws IOException {
                current = MetaTableAccessor.getRegionLocations(r);
                return super.visit(r);
            }

            @Override
            void processResult(Result r) {
                if (current == null) {
                    return;
                }
                for (HRegionLocation loc : current.getRegionLocations()) {
                    if (loc != null && !loc.getRegionInfo().isOffline()) {
                        if (serverName == null) {
                            this.results.add(loc.getRegionInfo());
                        } else if (loc.getServerName() != null && Objects.equals(serverName.getHostAndPort(), loc.getServerName().getHostAndPort())) {
                            this.results.add(loc.getRegionInfo());
                        }
                    }
                }
            }
        };
        try {
            fullScan(connection, visitor, new byte[0]);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return visitor.getResults();
    }

    private static void fullScan(Connection connection, final MetaTableAccessor.Visitor visitor, final byte[] startrow) throws IOException {
        Scan scan = new Scan();
        if (startrow != null)
            scan.setStartRow(startrow);
        if (startrow == null) {
            int caching = connection.getConfiguration().getInt(HConstants.HBASE_META_SCANNER_CACHING, 10000);
            scan.setCaching(caching);
        }
        scan.addFamily(HConstants.CATALOG_FAMILY);
        try (Table metaTable = connection.getTable(TableName.META_TABLE_NAME); ResultScanner scanner = metaTable.getScanner(scan)) {
            Result data;
            while ((data = scanner.next()) != null) {
                if (data.isEmpty())
                    continue;
                if (!visitor.visit(data))
                    break;
            }
        }
    }

    static abstract class CollectingVisitor<T> implements MetaTableAccessor.Visitor {
        final ServerName serverName;
        final Set<T> results = new HashSet<>();

        CollectingVisitor(ServerName sn) {
            this.serverName = sn;
        }

        @Override
        public boolean visit(Result r) throws IOException {
            if (r == null || r.isEmpty()) return true;
            processResult(r);
            return true;
        }

        abstract void processResult(Result r);

        Set<T> getResults() {
            return this.results;
        }
    }

}
