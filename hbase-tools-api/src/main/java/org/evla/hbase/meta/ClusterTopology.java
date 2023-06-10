package org.evla.hbase.meta;

import org.apache.hadoop.hbase.ServerName;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ClusterTopology {
    public static final ClusterTopology EMPTY_TOPOLOGY = new ClusterTopology();

    private final Map<String, Set<String>> cluster = new HashMap<>();

    public ClusterTopology() {
    }

    public void mapRegionWithServer(ServerName serverName, String encodedRegionName) {
        mapRegionWithServer(serverName.getAddress().toString(), encodedRegionName);
    }

    public void mapRegionWithServer(String serverName, String encodedRegionName) {
        Set<String> set = cluster.computeIfAbsent(serverName, k -> new HashSet<>());
        set.add(encodedRegionName);
    }

    public Set<String> getAllServers() {
        return cluster.keySet();
    }

    public Set<String> getAllRegions(ServerName serverName) {
        return getAllRegions(serverName.getAddress().toString());
    }

    public Set<String> getAllRegions(String serverName) {
        return cluster.get(serverName);
    }

    public int getAllRegionsCount() {
        AtomicInteger counter = new AtomicInteger(0);
        for (Set<String> set : cluster.values()) {
            counter.addAndGet(set.size());
        }
        return counter.get();
    }

    public static Difference calculateRegionsDiffForServer(ClusterTopology first, ClusterTopology second, ServerName serverName) {
        Set<String> currentRegions = new HashSet<>(first.getAllRegions(serverName));
        Set<String> previousRegions = new HashSet<>(second.getAllRegions(serverName));

        currentRegions.removeAll(new HashSet<>(second.getAllRegions(serverName)));
        previousRegions.removeAll(new HashSet<>(first.getAllRegions(serverName)));

        List<DiffRecord> diff = new ArrayList<>();
        currentRegions.forEach(s -> diff.add(new DiffRecord(DiffType.IN, s)));
        previousRegions.forEach(s -> diff.add(new DiffRecord(DiffType.OUT, s)));

        return Difference.ofRegions(diff);
    }

    public static Difference calculateServersDiff(ClusterTopology first, ClusterTopology second) {
        Set<String> currentRegions = new HashSet<>(first.getAllServers());
        Set<String> previousRegions = new HashSet<>(second.getAllServers());

        currentRegions.removeAll(new HashSet<>(second.getAllServers()));
        previousRegions.removeAll(new HashSet<>(first.getAllServers()));

        List<DiffRecord> diff = new ArrayList<>();
        currentRegions.forEach(s -> diff.add(new DiffRecord(DiffType.IN, s)));
        previousRegions.forEach(s -> diff.add(new DiffRecord(DiffType.OUT, s)));

        return Difference.ofRegionServers(diff);
    }
}
