package org.evla.hbase.compactor;


import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.RegionMetrics;

public class CompactionWeight implements Comparable<CompactionWeight> {
    private final String encodedRegionName;
    private final Float locality;
    private final int storeFileCount;
    private final int totalRegionSize;
    private final int maxStoreFile;

    @Deprecated
    public CompactionWeight(RegionLoad load) {
        this.encodedRegionName = load.getNameAsString();
        this.locality = load.getDataLocality();
        this.storeFileCount = load.getStorefiles();
        this.totalRegionSize = load.getStorefileSizeMB();
        this.maxStoreFile = 0;
    }

    public CompactionWeight(RegionMetrics regionMetrics) {
        this.encodedRegionName = regionMetrics.getNameAsString();
        this.locality = regionMetrics.getDataLocality();
        this.storeFileCount = regionMetrics.getStoreFileCount();
        this.totalRegionSize = (int) regionMetrics.getStoreFileSize().getLongValue();
        this.maxStoreFile = 0;
    }

    CompactionWeight(String encodedRegionName, Float locality, int storeFileCount, int totalRegionSize, int maxStoreFile) {
        this.encodedRegionName = encodedRegionName;
        this.locality = locality;
        this.storeFileCount = storeFileCount;
        this.totalRegionSize = totalRegionSize;
        this.maxStoreFile = maxStoreFile;
    }

    public Float calculateRegionCompactionWeight() {
        if (totalRegionSize < 10)
            return 0f;

        return (1 - locality) * 115f + ((totalRegionSize - maxStoreFile) / 1024f) * (storeFileCount * 1.33f);
    }

    @Override
    public String toString() {
        return "Weight => " +
                "encodedRegionName=" + encodedRegionName +
                ", calculatedWeight=" + calculateRegionCompactionWeight() +
                ", locality=" + locality +
                ", storeFileCount=" + storeFileCount +
                ", totalRegionSize=" + totalRegionSize +
                ", maxStoreFile=" + maxStoreFile +
                '}';
    }

    @Override
    public int compareTo(CompactionWeight anotherWeight) {
        return this.calculateRegionCompactionWeight().compareTo(anotherWeight.calculateRegionCompactionWeight());
    }

    public String getEncodedRegionName() {
        return encodedRegionName;
    }

    public Float getLocality() {
        return locality;
    }

    public int getStoreFileCount() {
        return storeFileCount;
    }

    public int getTotalRegionSize() {
        return totalRegionSize;
    }

    public int getMaxStoreFile() {
        return maxStoreFile;
    }

}
