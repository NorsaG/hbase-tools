package org.evla.hbase.rstask;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class RSTaskControllerHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(RSTaskControllerHelper.class);

    public static <K> Map<ServerName, RSTaskController<K>> getControllers(Admin admin, int threads) {
        Map<ServerName, RSTaskController<K>> controllers = new HashMap<>();
        try {
            admin.getClusterMetrics().getServersName().forEach(sn -> controllers.put(sn, new RSTaskController<>(sn, threads)));
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return controllers;
    }

    public static boolean isRegionCompacting(Admin admin, RegionInfo info) throws IOException {
        try {
            CompactionState state = admin.getCompactionStateForRegion(info.getRegionName());
            return state != CompactionState.NONE && state != CompactionState.MINOR;
        } catch (NotServingRegionException e) {
            LOGGER.error("Region {} can`t compact because region not online. {}", info, e.getMessage());
            throw e;
        } catch (Exception e) {
            // here is the reason for the large number of reads
            // we shouldn't skip this error and rethrow it to caller
            // owervise we will repeatably read from hbase:meta (HBaseAdmin -> getCompactionStateForRegion() -> getRegion())
            // for non-existing region (in most of the cases)
            LOGGER.error("Region " + info + " cant be split. " + e.getMessage(), e);
            throw e;
        }
    }

    public static boolean isTableCompacting_checked(Admin admin, TableName tn, long statusDelay) {
        Boolean result = null;
        int retries = 100;
        while (result == null) {
            try {
                CompactionState state = admin.getCompactionState(tn);
                result = state != CompactionState.NONE && state != CompactionState.MINOR;
            } catch (Exception e) {
                // skip it
                retries--;
                sleep(statusDelay);
            }
            if (retries < 0) {
                return false;
            }
        }

        return result;
    }

    public static boolean isTableCompacting(Admin admin, TableName tn) throws IOException {
        try {
            CompactionState state = admin.getCompactionState(tn);
            return state != CompactionState.NONE && state != CompactionState.MINOR;
        } catch (NotServingRegionException e) {
            LOGGER.error("Table {} can`t compact because region not online. {}", tn, e.getMessage());
            throw e;
        } catch (Exception e) {
            // here is the reason for the large number of reads
            // we shouldn't skip this error and rethrow it to caller
            // owervise we will repeatably read from hbase:meta (HBaseAdmin -> getCompactionStateForRegion() -> getRegion())
            // for non-existing region (in most of the cases)
            LOGGER.error("Table " + tn + " cant be split. " + e.getMessage(), e);
            throw e;
        }
    }

    public static void waitUntilCompacting(Admin admin, HRegionInfo info, long statusDelay) throws IOException {
        LOGGER.info("Wait while compacting region {}", info);
        boolean isCompacting = RSTaskControllerHelper.isRegionCompacting(admin, info);
        while (isCompacting) {
            sleep(statusDelay);
            isCompacting = RSTaskControllerHelper.isRegionCompacting(admin, info);
        }
    }

    public static void waitUntilCompacting(Admin admin, TableName tn, long statusDelay) throws IOException {
        LOGGER.info("Wait while compacting table {}", tn);
        boolean isCompacting = RSTaskControllerHelper.isTableCompacting(admin, tn);
        while (isCompacting) {
            sleep(statusDelay);
            isCompacting = RSTaskControllerHelper.isTableCompacting(admin, tn);
        }
    }

    public static void waitUntilCompacting_checked(Admin admin, TableName tn, long statusDelay) {
        LOGGER.info("Wait for compacting table {}", tn);
        boolean isCompacting = RSTaskControllerHelper.isTableCompacting_checked(admin, tn, statusDelay);
        while (isCompacting) {
            sleep(statusDelay);
            isCompacting = RSTaskControllerHelper.isTableCompacting_checked(admin, tn, statusDelay);
        }
    }

    public static void waitUntilRegionsReadyToSplit(Admin admin, TableName tn) throws IOException {
        //
    }

    public static void waitWhileSplitting(Admin admin, TableName tn) {
        LOGGER.info("Wait for splitting table {}", tn);
        try {
            List<RegionInfo> regionInfoList = admin.getRegions(tn);
            boolean isTransit = RSTaskControllerHelper.isRegionsInTransition(admin, regionInfoList);
            while (isTransit) {
                RSTaskControllerHelper.sleep(3_000);
                isTransit = RSTaskControllerHelper.isRegionsInTransition(admin, regionInfoList);
            }
        } catch (Exception e) {
            LOGGER.error("Cant get info about table regions: " + tn, e);
        }
    }

    public static Map<String, RegionState> getRegionsInTransitionMap(ClusterMetrics metrics) {
        return metrics.getRegionStatesInTransition().stream().collect(Collectors.toMap(rs -> rs.getRegion().getEncodedName(), rs -> rs));
    }

    public static boolean isRegionsInTransition(Admin admin, List<RegionInfo> regions) throws IOException {
        try {
            Map<String, RegionState> regionsInTransition = getRegionsInTransitionMap(admin.getClusterStatus());
            for (RegionInfo info : regions) {
                if (regionsInTransition.containsKey(info.getEncodedName())) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            throw e;
        }
    }

    public static void moveRegion(Admin admin, RegionInfo info, ServerName destServer) throws IOException {
        admin.move(info.getEncodedNameAsBytes(), destServer.getServerName().getBytes());
        LOGGER.info("Region {} moved to {} server", info.getEncodedName(), destServer);
    }

    public static Map<ServerName, Long> calculateTableDistribution(Admin admin, List<ServerName> allServers, TableName tn) {
        int repeat = 5;
        while (repeat > 0) {
            try {
                List<HRegionLocation> locations = admin.getConnection().getRegionLocator(tn).getAllRegionLocations();
                return calculateTableDistribution_repeatable(allServers, locations);
            } catch (Exception e) {
                repeat--;
                LOGGER.warn("Cant get distribution for: {}", tn);
                RSTaskControllerHelper.sleep(3_000);
            }
        }
        return Collections.emptyMap();
    }

    private static Map<ServerName, Long> calculateTableDistribution_repeatable(List<ServerName> allServers, List<HRegionLocation> locations) {
        Map<ServerName, Long> distribution = new HashMap<>(locations.stream().collect(Collectors.groupingBy(HRegionLocation::getServerName, Collectors.counting())));
        for (ServerName sn : allServers) {
            distribution.putIfAbsent(sn, 0L);
        }
        return distribution;
    }

    public static void sleep(long timeout) {
        try {
            Thread.sleep(timeout);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    private static final Map<ServerName, HBaseRpcController> controllers = new HashMap<>();

    private static HBaseRpcController getController(Admin admin, ServerName serverName) {
        controllers.computeIfAbsent(serverName, (sn) -> ((ClusterConnection) admin.getConnection()).getRpcControllerFactory().newController());
        return controllers.get(serverName);
    }

    private static AdminProtos.GetRegionInfoResponse getRemoteRegionInfo(Admin admin, ServerName serverName, RegionInfo regionInfo) {
        HBaseRpcController controller = getController(admin, serverName);
        AdminProtos.GetRegionInfoRequest request = RequestConverter.buildGetRegionInfoRequest(regionInfo.getRegionName(), true, true);
        try {
            return ((ClusterConnection) admin.getConnection()).getAdmin(serverName).getRegionInfo(controller, request);
        } catch (Exception e) {
            controllers.remove(serverName);
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    public static boolean isRegionSplittable(Admin admin, HRegionLocation location) {
        return isRegionSplittable(admin, location.getServerName(), location.getRegion());
    }

    public static boolean isRegionSplittable(Admin admin, ServerName serverName, RegionInfo regionInfo) {
        AdminProtos.GetRegionInfoResponse response = getRemoteRegionInfo(admin, serverName, regionInfo);
        return response != null && response.getSplittable();
    }

    public static boolean isRegionMergeable(Admin admin, HRegionLocation location) {
        return isRegionMergeable(admin, location.getServerName(), location.getRegion());
    }

    public static boolean isRegionMergeable(Admin admin, ServerName serverName, RegionInfo regionInfo) {
        AdminProtos.GetRegionInfoResponse response = getRemoteRegionInfo(admin, serverName, regionInfo);
        return response != null && response.getMergeable();
    }

}
