package org.evla.hbase.distributor;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.master.RegionState;
import org.evla.hbase.rstask.RSTaskControllerHelper;
import org.evla.hbase.compactor.Compactor;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TableDistributor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableDistributor.class);

    private final Admin admin;
    private final Compactor compactor;
    private final HBaseToolsSettings settings;


    TableDistributor(Admin admin, HBaseToolsSettings settings, Compactor compactor) {
        this.admin = admin;
        this.settings = settings;
        this.compactor = compactor;
    }

    void distributeTable(List<ServerName> serversList, TableName tn) {
        try {
            RegionLocator locator = admin.getConnection().getRegionLocator(tn);
            List<HRegionLocation> locations = locator.getAllRegionLocations();
            LOGGER.info("Re-balance {} table regions", tn);

            if (locations.size() < serversList.size()) {
                distributeSmallTable(serversList, locations);
            } else {
                distributeBigTable(serversList, tn);
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void distributeSmallTable(List<ServerName> serversList, List<HRegionLocation> locations) throws IOException {
        Set<ServerName> usedServers = new HashSet<>();
        List<HRegionLocation> locationsForMove = new ArrayList<>();
        locations.forEach(h -> {
            if (usedServers.contains(h.getServerName())) {
                locationsForMove.add(h);
            } else {
                usedServers.add(h.getServerName());
            }
        });
        serversList.removeAll(usedServers);
        Collections.shuffle(serversList);
        int i = 0;
        for (HRegionLocation l : locationsForMove) {
            RSTaskControllerHelper.moveRegion(admin, l.getRegionInfo(), serversList.get(i));
            waitMoving(Collections.singletonList(l.getRegionInfo()));

            compactRegion(l);
            i++;
        }
    }

    private void distributeBigTable(List<ServerName> serversList, TableName tn) throws IOException {
        HRegionLocation maxServerRegion;
        ServerName destinationServer;
        while (true) {
            List<HRegionLocation> locations = admin.getConnection().getRegionLocator(tn).getAllRegionLocations();
            Map<ServerName, Long> distribution = RSTaskControllerHelper.calculateTableDistribution(admin, serversList, tn);
            maxServerRegion = getBiggestRegion(distribution, locations);
            destinationServer = getSmallestServer(distribution);

            if (distribution.get(maxServerRegion.getServerName()) - distribution.get(destinationServer) > 1) {
                RSTaskControllerHelper.moveRegion(admin, maxServerRegion.getRegionInfo(), destinationServer);
                waitMoving(Collections.singletonList(maxServerRegion.getRegionInfo()));

                compactRegion(maxServerRegion);
            } else {
                break;
            }
        }
    }

    private void compactRegion(HRegionLocation regionLocation) {
        if (compactor != null) {
            compactor.compactRegions(Collections.singletonList(regionLocation));
        }
    }


    private HRegionLocation getBiggestRegion(Map<ServerName, Long> distribution, List<HRegionLocation> locations) {
        Optional<Map.Entry<ServerName, Long>> sn = distribution.entrySet().stream().max(Comparator.comparingLong(Map.Entry::getValue));
        ServerName serverName = sn.map(Map.Entry::getKey).orElse(null);

        for (HRegionLocation l : locations) {
            if (l.getServerName().equals(serverName)) {
                return l;
            }
        }
        throw new RuntimeException("Incorrect behavior");
    }

    private ServerName getSmallestServer(Map<ServerName, Long> distribution) {
        Optional<Map.Entry<ServerName, Long>> sn = distribution.entrySet().stream().min(Comparator.comparingLong(Map.Entry::getValue));
        return sn.orElseThrow(() -> new RuntimeException("Incorrect behavior")).getKey();
    }

    private void waitMoving(List<HRegionInfo> regions) throws IOException {
        while (true) {
            Map<String, RegionState> states = RSTaskControllerHelper.getRegionsInTransitionMap(admin.getClusterMetrics());
            List<String> regionsNames = regions.stream().map(HRegionInfo::getEncodedName).collect(Collectors.toList());
            boolean needWait = false;
            for (String region : regionsNames) {
                if (states.containsKey(region)) {
                    RSTaskControllerHelper.sleep(settings.getDistributorSettings().getDistributorRITTimeout());
                    needWait = true;
                    break;
                }
            }
            if (!needWait) {
                break;
            }
        }

    }
}
