package org.evla.hbase.merger;

public class MergeParams {
    private final long minStorefileSize_mb;
    private final long maxStorefileSize_mb;
    private final long maxMergedStorefileSize_mb;

    public MergeParams(long minStorefileSize_mb, long maxStorefileSize_mb, long maxMergedStorefileSize_mb) {
        this.minStorefileSize_mb = minStorefileSize_mb;
        this.maxStorefileSize_mb = maxStorefileSize_mb;
        this.maxMergedStorefileSize_mb = maxMergedStorefileSize_mb;
    }

    public long getMinStorefileSize_mb() {
        return minStorefileSize_mb;
    }

    public long getMaxStorefileSize_mb() {
        return maxStorefileSize_mb;
    }

    public long getMaxMergedStorefileSize_mb() {
        return maxMergedStorefileSize_mb;
    }
}
