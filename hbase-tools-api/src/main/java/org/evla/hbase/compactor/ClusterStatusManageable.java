package org.evla.hbase.compactor;

import org.apache.hadoop.hbase.ClusterStatus;

public interface ClusterStatusManageable {
    ClusterStatus getClusterStatus();
}
