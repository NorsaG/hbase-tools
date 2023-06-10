package org.evla.hbase.merger;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.common.HBaseStaticHelper;
import org.evla.hbase.compactor.Compactor;
import org.evla.hbase.compactor.LightweightCompactor;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class SafeMerger {
    private static final Logger LOGGER = LoggerFactory.getLogger(SafeMerger.class);
    private static final int MAX_RETRIES = 10;

    private final Admin admin;

    private final Merger merger;
    private HBaseToolsSettings settings = null;

    public SafeMerger(Admin admin, QualityMerge qualityMerge, MergeParams mergeParams) {
        this.admin = admin;
        this.merger = new Merger(admin, qualityMerge, mergeParams);
    }

    public SafeMerger(Admin admin, HBaseToolsSettings properties) {
        this.admin = admin;
        this.settings = properties;
        this.merger = new Merger(admin, properties);
    }

    public void run(String table, int border) {
        run(table, border, merger.isCheckSnp);
    }

    public void run(String table, int border, boolean checkSnapshotExists) {
        LOGGER.info("Start safe merging of {}", table);
        TableName originalTable = TableName.valueOf(table);
        TableName tn = HBaseStaticHelper.createTableCopy(admin, originalTable, "copy_" + originalTable.toString().replaceAll(":", "_"));

        if (tn != null) {
            LOGGER.info("Compact copy table: {}", tn);
            try (Compactor compactor = new LightweightCompactor(admin, settings)) {
                compactor.compactTables(Collections.singletonList(tn));
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            LOGGER.info("Stop compacting copy table: {}", tn);

            merger.run(tn.toString(), border, checkSnapshotExists);
            LOGGER.info("Stop safe merging of {}", table);
        } else {
            LOGGER.warn("Cant create copy of '{}' table. Stop safe merging.", table);
        }
    }


}
