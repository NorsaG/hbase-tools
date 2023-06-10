package org.evla.hbase.common;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.HBaseToolsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HBaseStaticHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStaticHelper.class);

    private static final DateTimeFormatter NAME_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    public static TableName createTableCopy(Admin admin, TableName originalTable, String nameOfCopy) {
        LOGGER.info("Start creating table copy: {} => {}", originalTable, nameOfCopy);
        TableName createdTable;
        String snpName = String.join("_", "snp", originalTable.toString(), LocalDateTime.now().format(NAME_DATE_FORMATTER)).replaceAll(":", "_");

        boolean snapshotExists = createSnapshot(admin, originalTable, snpName);

        if (snapshotExists) {
            createdTable = cloneSnapshot(admin, TableName.valueOf(nameOfCopy), snpName);
            HBaseToolsHelper.deleteSnapshot(admin, snpName);
            if (createdTable != null) {
                LOGGER.info("Created table: {}", createdTable);
                return createdTable;
            } else {
                LOGGER.warn("Table {} not cloned", nameOfCopy);
                return null;
            }
        } else {
            LOGGER.info("Snapshot '{}' for {} not created", snpName, originalTable);
            return null;
        }
    }

    public static boolean replaceTable(Admin admin, TableName fromTable, TableName toTable, boolean makeCopy) {
        LOGGER.info("Start replacing tables from => to : {} => {}", fromTable, toTable);
        TableName copyTableName = constructCopyTableName(toTable);

        LOGGER.info("Start making copy for {}", toTable);
        TableName copiedTable = createTableCopy(admin, toTable, copyTableName.toString());
        if (copiedTable == null) {
            LOGGER.warn("Cant create copy table: {}", toTable);
            return false;
        }

        LOGGER.info("Delete destination table {}", toTable);
        boolean deleteTable = HBaseToolsHelper.deleteTable(admin, toTable);
        if (!deleteTable) {
            LOGGER.info("Destination table {} cant be deleted", toTable);
            return false;
        }

        TableName tn = createTableCopy(admin, fromTable, toTable.toString());
        if (tn == null) {
            LOGGER.warn("Table {} not copied", fromTable);
            return false;
        }
        deleteTable = HBaseToolsHelper.deleteTable(admin, fromTable);

        if (!deleteTable) {
            LOGGER.warn("Original table {} not deleted", fromTable);
        }

        if (!makeCopy) {
            boolean deleteCopy = HBaseToolsHelper.deleteTable(admin, copiedTable);
            if (!deleteCopy) {
                LOGGER.warn("Copy table {} cant be deleted", copiedTable);
            }
        }

        return true;
    }

    private static TableName constructCopyTableName(TableName toTable) {
        String namespace = toTable.getNamespaceAsString();
        String table = toTable.getQualifierAsString();

        String copyTable = String.join("_", "origin", table, LocalDateTime.now().format(NAME_DATE_FORMATTER));
        return TableName.valueOf(namespace, copyTable);
    }

    private static TableName cloneSnapshot(Admin admin, TableName newTableName, String snapshotName) {
        try {
            LOGGER.info("Start clone snapshot '{}' in table '{}'", snapshotName, newTableName);
            admin.cloneSnapshot(snapshotName, newTableName);
            LOGGER.info("Table '{}' successfully cloned from '{}'", newTableName, snapshotName);
            return newTableName;
        } catch (IOException e) {
            LOGGER.error("Snapshot '{}' for {} not cloned. {}", snapshotName, newTableName, e.getMessage());
            return null;
        }
    }

    private static boolean createSnapshot(Admin admin, TableName tableName, String snapshotName) {
        try {
            LOGGER.info("Create '{}'-snapshot for table {}", snapshotName, tableName);
            admin.snapshot(snapshotName, tableName);
            LOGGER.info("Snapshot '{}' successfully created for table {}", snapshotName, tableName);
            return true;
        } catch (Exception e) {
            LOGGER.error("Snapshot '{}' for {} not complete. {}", snapshotName, tableName, e.getMessage());
            return false;
        }
    }

}
