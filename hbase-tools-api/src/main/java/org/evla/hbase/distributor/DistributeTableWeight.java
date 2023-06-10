package org.evla.hbase.distributor;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributeTableWeight implements Comparable<DistributeTableWeight> {
    private final TableName tableName;
    private final Map<ServerName, List<String>> distribution;

    private final int regions;

    public DistributeTableWeight(TableName tableName, Map<ServerName, List<String>> distribution) {
        this.tableName = tableName;
        this.distribution = distribution;

        this.regions = calcDistribution();
    }

    public TableName getTableName() {
        return tableName;
    }

    public Float getTableWeight() {
        Map<ServerName, Long> currentState = new HashMap<>();
        for (Map.Entry<ServerName, List<String>> entry : distribution.entrySet()) {
            currentState.put(entry.getKey(), (long) entry.getValue().size());
        }
        float weight = 0f;
        while (true) {
            ServerName max = getBiggestServer(currentState);
            ServerName min = getSmallestServer(currentState);
            if (currentState.get(max) - currentState.get(min) > 1) {
                Long lMax = currentState.get(max);
                Long lMin = currentState.get(min);

                currentState.put(max, lMax - 1);
                currentState.put(min, lMin + 1);
                weight++;
            } else {
                break;
            }
        }
        return weight;
    }

    public Map<ServerName, List<String>> getDistribution() {
        return distribution;
    }

    private int calcDistribution() {
        AtomicInteger regions = new AtomicInteger();
        getDistribution().values().forEach(c -> regions.addAndGet(c.size()));

        return regions.get();
    }

    public int getRegionsCount() {
        return regions;
    }

    @Override
    public String toString() {
        return "DistributeTableWeight{" +
                "tableName=" + tableName +
                ", regions=" + regions +
                ", weight=" + getTableWeight().intValue() +
                '}';
    }

    @Override
    public int compareTo(DistributeTableWeight o) {
        return this.getTableWeight().compareTo(o.getTableWeight());
    }

    private static ServerName getBiggestServer(Map<ServerName, Long> distribution) {
        Optional<Map.Entry<ServerName, Long>> sn = distribution.entrySet().stream().max(Comparator.comparingLong(Map.Entry::getValue));
        if (sn.isPresent())
            return sn.get().getKey();
        throw new RuntimeException("incorrect behavior");
    }

    private static ServerName getSmallestServer(Map<ServerName, Long> distribution) {
        Optional<Map.Entry<ServerName, Long>> sn = distribution.entrySet().stream().min(Comparator.comparingLong(Map.Entry::getValue));
        if (sn.isPresent())
            return sn.get().getKey();
        throw new RuntimeException("incorrect behavior");
    }
}
