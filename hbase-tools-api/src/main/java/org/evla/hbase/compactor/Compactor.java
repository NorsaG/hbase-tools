package org.evla.hbase.compactor;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;

import java.io.Closeable;
import java.util.List;
import java.util.Set;

public interface Compactor extends Closeable {

    void compactRegions(List<HRegionLocation> regions);

    void compactTables(List<TableName> tables);

    void compactNamespaces(Set<String> namespaces);

    void infiniteCompact();

    default void compact() {
    }
}
