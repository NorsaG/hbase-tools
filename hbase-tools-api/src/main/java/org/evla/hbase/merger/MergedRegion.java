package org.evla.hbase.merger;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.RegionInfo;

import java.util.Arrays;

public class MergedRegion {
    private final byte[] startKey;
    private final byte[] endKey;

    private MergedRegion(byte[] startKey, byte[] endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public static MergedRegion from(RegionInfo first, RegionInfo second) {
        return new MergedRegion(first.getStartKey(), second.getEndKey());
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergedRegion that = (MergedRegion) o;
        return Arrays.equals(startKey, that.startKey) && Arrays.equals(endKey, that.endKey);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(startKey);
        result = 31 * result + Arrays.hashCode(endKey);
        return result;
    }
}