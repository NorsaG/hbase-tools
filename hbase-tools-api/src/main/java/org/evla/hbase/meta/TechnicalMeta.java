package org.evla.hbase.meta;

import org.apache.commons.lang3.builder.Diff;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.types.CopyOnWriteArrayMap;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TechnicalMeta {
    private static final Logger LOGGER = LoggerFactory.getLogger(TechnicalMeta.class);
    private static final DateTimeFormatter DEFAULT_PATTERN = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final TableName technicalMetaTable;

    private final Admin admin;
    private final HBaseToolsSettings settings;
    private final MetaTableHolder holder = new MetaTableHolder();

    TechnicalMeta(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.settings = settings;
        this.technicalMetaTable = settings.getTechnicalMetaSettings().getTechnicalMetaTable();

        checkTechnicalTable(this.technicalMetaTable);
    }

    private void checkTechnicalTable(TableName technicalMetaTable) {
        try {
            LOGGER.info("Check settings for technical table: {}", technicalMetaTable);
            if (!admin.tableExists(technicalMetaTable)) {
                LOGGER.info("Technical meta-table not exists. Start creating");
                createTechnicalTable();
            } else {
                LOGGER.info("Technical meta-table exists. Checking table parameters");
                HTableDescriptor descriptor = admin.getTableDescriptor(technicalMetaTable);
                HColumnDescriptor[] cd = descriptor.getColumnFamilies();
                if (cd == null || cd.length > 1) {
                    LOGGER.info("Table contains not single column family");
                    dropTechnicalTable();
                    createTechnicalTable();
                } else {
                    HColumnDescriptor desc = cd[0];
                    if (desc.getTimeToLive() > settings.getTechnicalMetaSettings().getTechnicalMetaTableTTL()) {
                        LOGGER.info("Table have incorrect TLL settings");
                        dropTechnicalTable();
                        createTechnicalTable();
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void dropTechnicalTable() throws IOException {
        LOGGER.info("Disable technical meta table");
        admin.disableTable(technicalMetaTable);
        LOGGER.info("Drop technical meta table");
        admin.deleteTable(technicalMetaTable);
    }

    private void createTechnicalTable() throws IOException {
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf");
        columnDescriptor.setBloomFilterType(BloomType.ROW);
        columnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
        columnDescriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        columnDescriptor.setTimeToLive(settings.getTechnicalMetaSettings().getTechnicalMetaTableTTL());

        HTableDescriptor tableDescriptor = new HTableDescriptor(technicalMetaTable);
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);
    }

    public Difference getRegionsDiff(ClusterTopology current, ClusterTopology previousTopology, ServerName serverName) {
        return ClusterTopology.calculateRegionsDiffForServer(current, previousTopology, serverName);
    }

    public Difference getRegionsDiff(ServerName serverName, Long timestamp, boolean updateState) {
        ClusterTopology current = getActualTopology(updateState);
        ClusterTopology previousTopology = getTopology(timestamp);

        return getRegionsDiff(current, previousTopology, serverName);
    }

    public Difference getServerDiff(ClusterTopology current, ClusterTopology previousTopology) {
       return ClusterTopology.calculateServersDiff(current, previousTopology);
    }

    public Difference getServerDiff(Long timestamp, boolean updateState) {
        ClusterTopology current = getActualTopology(updateState);
        ClusterTopology previousTopology = getTopology(timestamp);

        return getServerDiff(current, previousTopology);
    }

    public ClusterTopology getActualTopology() {
        return getActualTopology(false);
    }

    public ClusterTopology getActualTopology(boolean updateState) {
        try {
            Map<ServerName, Set<HRegionInfo>> meta = holder.getAllRegions(admin.getClusterStatus().getServers(), admin.getConnection());
            LOGGER.info("Get current cluster topology");
            ClusterTopology topology = getClusterState(meta);

            if (updateState) {
                LOGGER.info("Store actual cluster topology");
                updateState(topology);
            }

            return topology;
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    public ClusterTopology getTopology(long timestamp) {
        NavigableMap<Long, ClusterTopology> history = new CopyOnWriteArrayMap<>(Long::compareTo);
        LOGGER.info("Reading cluster topology");
        Scan scan = new Scan();
        try {
            ResultScanner resultScanner = admin.getConnection().getTable(technicalMetaTable).getScanner(scan);
            Result result;
            while ((result = resultScanner.next()) != null) {
                while (result.advance()) {
                    Cell cell = result.current();
                    String regionName = new String(CellUtil.cloneRow(cell));
                    String colName = new String(CellUtil.cloneQualifier(cell));
                    if (!colName.startsWith("server_")) {
                        continue;
                    }
                    String[] parts = colName.split("_");
                    Long time = Long.parseLong(parts[1]);
                    ClusterTopology ct = history.computeIfAbsent(time, t -> new ClusterTopology());

                    String serverValue = new String(CellUtil.cloneValue(cell));
                    ct.mapRegionWithServer(ServerName.parseServerName(serverValue), regionName);
                }
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        Map.Entry<Long, ClusterTopology> entry = history.floorEntry(timestamp);
        return entry != null ? entry.getValue() : ClusterTopology.EMPTY_TOPOLOGY;
    }

    private void updateState(ClusterTopology topology) {
        long timestamp = System.currentTimeMillis();

        LOGGER.info("Update topology state");
        writeTopology(topology, timestamp);
    }

    private void writeTopology(ClusterTopology topology, long timestamp) {
        try {
            BufferedMutator mutator = admin.getConnection().getBufferedMutator(technicalMetaTable);
            for (String sn : topology.getAllServers()) {
                for (String regionName : topology.getAllRegions(sn)) {
                    Put put = new Put(regionName.getBytes(StandardCharsets.UTF_8));
                    put.setTTL(settings.getTechnicalMetaSettings().getTechnicalMetaTableRecordTTL());
                    put.addColumn("cf".getBytes(StandardCharsets.UTF_8), ("server_" + timestamp).getBytes(StandardCharsets.UTF_8),
                            sn.getBytes(StandardCharsets.UTF_8));

                    mutator.mutate(put);
                }
            }
            mutator.flush();
            LocalDateTime ldt = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            LOGGER.info("Cluster topology successfully stored in {} at {}({})", technicalMetaTable, timestamp, DEFAULT_PATTERN.format(ldt));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private ClusterTopology getClusterState(Map<ServerName, Set<HRegionInfo>> meta) {
        if (meta == null) {
            return null;
        }
        ClusterTopology clusterTopology = new ClusterTopology();

        for (Map.Entry<ServerName, Set<HRegionInfo>> rs : meta.entrySet()) {
            for (HRegionInfo info : rs.getValue()) {
                clusterTopology.mapRegionWithServer(rs.getKey(), info.getRegionNameAsString());
            }
        }
        return clusterTopology;
    }
}
