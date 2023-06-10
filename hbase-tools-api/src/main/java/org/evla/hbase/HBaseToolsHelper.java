package org.evla.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HBaseToolsHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsHelper.class);

    public static boolean deleteTable(Admin admin, TableName tableName) {
        LOGGER.info("Start delete table {}", tableName);
        try {
            admin.disableTable(tableName);
        } catch (IOException e) {
            LOGGER.error("Cant disable table {}. {}", tableName, e.getMessage());
            return false;
        }
        try {
            admin.deleteTable(tableName);
        } catch (IOException e) {
            LOGGER.error("Cant delete table {}. {}", tableName, e.getMessage());
            try {
                admin.enableTable(tableName);
            } catch (IOException ex) {
                LOGGER.error("Cant enable table {}. {}", tableName, ex.getMessage());
                throw new RuntimeException(ex);
            }
            return false;
        }

        LOGGER.info("Table {} deleted", tableName);
        return true;
    }

    public static void deleteSnapshot(Admin admin, String snapshotName) {
        try {
            LOGGER.info("Start delete '{}' snapshot", snapshotName);
            admin.deleteSnapshot(snapshotName);
            LOGGER.info("Snapshot '{}' successfully deleted", snapshotName);
        } catch (IOException e) {
            LOGGER.error("Snapshot '{}' not deleted. {}", snapshotName, e.getMessage());
        }
    }

    public static ServerName selectServerName(Collection<ServerName> servers, String serverStr) {
        Set<ServerName> filteredNames = findServerNames(servers, serverStr);
        if (filteredNames.isEmpty()) {
            throw new RuntimeException("Server Name cant be parsed");
        } else if (filteredNames.size() > 1) {
            throw new RuntimeException("Several Server Names found by " + serverStr);
        }
        return filteredNames.stream().findFirst().get();
    }

    public static ServerName selectServerName(Admin admin, String serverStr) {
        Set<ServerName> names;
        try {
            names = findServerNames(admin, serverStr);
            if (names.isEmpty()) {
                throw new RuntimeException("Server Name cant be parsed");
            } else if (names.size() > 1) {
                throw new RuntimeException("Several Server Names found by " + serverStr);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return names.stream().findFirst().get();
    }

    public static Set<ServerName> findServerNames(Collection<ServerName> servers, String name) {
        if (StringUtils.isBlank(name)) {
            return Collections.emptySet();
        }
        Set<ServerName> validServers = new HashSet<>();
        for (ServerName sn : servers) {
            if (sn.getHostAndPort().startsWith(name)) {
                validServers.add(sn);
            }
        }
        return validServers;
    }

    public static Set<ServerName> findServerNames(Admin admin, String name) throws IOException {
        return findServerNames(admin.getClusterStatus().getServers(), name);
    }
}
