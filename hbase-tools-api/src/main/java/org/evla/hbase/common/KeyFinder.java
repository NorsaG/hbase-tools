package org.evla.hbase.common;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.types.CopyOnWriteArrayMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KeyFinder {
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyFinder.class);
    private final TableName tableName;

    public KeyFinder(TableName tn) {
        this.tableName = tn;
    }

    public void findLocation(Admin admin, String key) {
        HRegionLocation loc;
        try {
            loc = findLocationForKey(admin, key);
            System.out.println("Key: \"" + KeyGenerator.toStringBinary(KeyGenerator.generateKeyAsBytes(key)) + "\"");
            System.out.println("Region: " + loc.getRegionInfo().getRegionNameAsString());
            System.out.println("host: " + loc.getHostnamePort());
            System.out.println("startKey: " + KeyGenerator.toStringBinary(loc.getRegionInfo().getStartKey()));
            System.out.println("endKey: " + KeyGenerator.toStringBinary(loc.getRegionInfo().getEndKey()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public HRegionLocation findLocationForKey(Admin admin, String key) throws IOException {
        HRegionLocation location = ((ClusterConnection) admin.getConnection()).locateRegion(tableName, KeyGenerator.generateKeyAsBytes(key));

        if (location == null) {
            throw new RuntimeException("Region not found for " + tableName);
        }

        return location;
    }
}
