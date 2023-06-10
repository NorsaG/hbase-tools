package org.evla.hbase.meta;


import java.util.List;

import static org.evla.hbase.meta.Difference.Type.REGION;
import static org.evla.hbase.meta.Difference.Type.REGION_SERVER;

public class Difference {
    private final Type type;
    private final List<DiffRecord> records;


    Difference(Type type, List<DiffRecord> records) {
        this.type = type;
        this.records = records;
    }

    public static Difference ofRegionServers(List<DiffRecord> servers) {
        return new Difference(REGION_SERVER, servers);
    }

    public static Difference ofRegions(List<DiffRecord> regions) {
        return new Difference(REGION, regions);
    }

    public Type getType() {
        return type;
    }

    public List<DiffRecord> getDifference() {
        return records;
    }

    public enum Type {
        REGION_SERVER, REGION
    }

}
