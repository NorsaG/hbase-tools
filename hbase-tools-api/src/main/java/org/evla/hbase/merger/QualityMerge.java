package org.evla.hbase.merger;

import org.apache.hadoop.hbase.RegionMetrics;

public enum QualityMerge {
    SMALL {
        @Override
        public void setBorderRegionsCount(int i) {
            borderRegionsCount = -1;
        }

        @Override
        public boolean canMergeRegions(RegionMetrics a, RegionMetrics b, MergeParams p) {
            long storeSizeA = a.getStoreFileSize().getLongValue();
            long storeSizeB = b.getStoreFileSize().getLongValue();

            return (storeSizeA < p.getMinStorefileSize_mb() && storeSizeB < p.getMaxStorefileSize_mb()) ||
                    (storeSizeB < p.getMinStorefileSize_mb() && storeSizeA < p.getMaxStorefileSize_mb());
        }
    }, MEDIUM {
        @Override
        public boolean canMergeRegions(RegionMetrics a, RegionMetrics b, MergeParams p) {
            return canMergeTwoRegions(a, b, p);
        }

        @Override
        public void setBorderRegionsCount(int i) {
            borderRegionsCount = i;
        }
    }, LARGE {
        @Override
        public boolean canMergeRegions(RegionMetrics a, RegionMetrics b, MergeParams p) {
            return canMergeTwoRegions(a, b, p);
        }

        @Override
        public void setBorderRegionsCount(int i) {
            borderRegionsCount = 1;
        }
    };

    int borderRegionsCount;

    public int getBorderRegionsCount() {
        return borderRegionsCount;
    }

    public abstract boolean canMergeRegions(RegionMetrics a, RegionMetrics b, MergeParams p);

    public abstract void setBorderRegionsCount(int i);

    public static QualityMerge parseQuality(String quality) {
        switch (quality.toLowerCase()) {
            case "на-пол-шишечки":
            case "small": {
                return SMALL;
            }
            case "ну-сожми-чуток":
            case "medium": {
                return MEDIUM;
            }
            case "по-взрослому":
            case "large": {
                return LARGE;
            }
        }
        throw new RuntimeException("Incorrect merge parameters.");
    }

    private static boolean canMergeTwoRegions(RegionMetrics a, RegionMetrics b, MergeParams p) {
        long storeSizeA = a.getStoreFileSize().getLongValue();
        long storeSizeB = b.getStoreFileSize().getLongValue();

        if (storeSizeA < p.getMinStorefileSize_mb() || storeSizeB < p.getMinStorefileSize_mb())
            return true;

        if (storeSizeA > p.getMaxStorefileSize_mb())
            return false;

        if (storeSizeB > p.getMaxStorefileSize_mb())
            return false;

        return storeSizeA + storeSizeB <= p.getMaxMergedStorefileSize_mb();
    }
}