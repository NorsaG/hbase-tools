package org.evla.hbase.meta;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.evla.hbase.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TableLock {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableLock.class);

    private static final String LOCK_CF = "cf";
    private static final String COLUMN_LOCKED_BY = "LOCKED_BY";
    private static final String COLUMN_LOCK = "LOCK";

    private final long lockTTL;

    private final Table table;
    private final String processForLock;
    private final String pid;
    private final String locker;

    public TableLock(Table table, String processForLock, int pid, String resource, long lockTTL) {
        this.table = table;
        this.processForLock = processForLock;
        this.pid = String.valueOf(pid);
        this.locker = resource;
        this.lockTTL = lockTTL;
    }

    public boolean acquireLock() {
        LOGGER.info("Acquire lock for technical meta");
        Pair<String, String> lock = getCurrentTableLock();
        if (lock != null) {
            String foundPid = lock.getFirst();
            if (StringUtils.isNotBlank(foundPid)) {
                boolean checkLock = lock.getFirst().equals(pid);
                LOGGER.info("Lock already exists. Lock process owner: {}", checkLock);
                return checkLock;
            }
        }

        LOGGER.info("Try to get lock for: {}", processForLock);
        lockTable();

        lock = getCurrentTableLock();
        if (lock == null) {
            LOGGER.warn("Cant set lock for {}", processForLock);
            return false;
        }
        String foundPid = lock.getFirst();
        LOGGER.info("Get current lock for technical meta: {}", lock);
        return foundPid.equals(pid);
    }

    public void updateLock() {
        LOGGER.info("Update lock for technical meta");
        lockTable();
    }

    private void lockTable() {
        Put put = new Put(processForLock.getBytes(StandardCharsets.UTF_8));
        put.setTTL(lockTTL);
        put.addColumn(LOCK_CF.getBytes(StandardCharsets.UTF_8), COLUMN_LOCKED_BY.getBytes(StandardCharsets.UTF_8), pid.getBytes(StandardCharsets.UTF_8));
        put.addColumn(LOCK_CF.getBytes(StandardCharsets.UTF_8), COLUMN_LOCK.getBytes(StandardCharsets.UTF_8), locker.getBytes(StandardCharsets.UTF_8));
        try {
            table.put(put);
            LOGGER.info("Lock successfully updated");
        } catch (IOException e) {
            LOGGER.error("Cant lock {}. Exception: {}", processForLock, e.getMessage());
        }
    }

    private Pair<String, String> getCurrentTableLock() {
        Get get = new Get(processForLock.getBytes(StandardCharsets.UTF_8));
        Result r = null;
        try {
            r = table.get(get);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        if (r == null || r.isEmpty()) {
            LOGGER.info("Lock for {} not exists in lock-table", processForLock);
            return null;
        }
        String pid = null;
        String resource = null;
        while (r.advance()) {
            Cell cell = r.current();
            String colName = new String(CellUtil.cloneQualifier(cell));
            if (COLUMN_LOCKED_BY.equals(colName)) {
                pid = new String(CellUtil.cloneValue(cell));
            }
            if (COLUMN_LOCK.equals(colName)) {
                resource = new String(CellUtil.cloneValue(cell));
            }
        }

        return new Pair<>(pid, resource);
    }

}
