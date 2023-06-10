package org.evla.hbase.tasks;

import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.rstask.RSTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Should be rethought
 */
@Deprecated
public class MergeTask extends RSTask<Void> implements Comparable<MergeTask> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeTask.class);

    private final Admin admin;
    private final String firstRegionName;
    private final String secondRegionName;
    private final ConcurrentHashMap<String, String> regionsUnderMerge;

    public MergeTask(Admin admin, String firstRegionName, String secondRegionName, ConcurrentHashMap<String, String> regionsUnderMerge) {
        this.admin = admin;
        this.firstRegionName = firstRegionName;
        this.secondRegionName = secondRegionName;
        this.regionsUnderMerge = regionsUnderMerge;
    }

    @Override
    public Void executeTask() {
        try {
            LOGGER.info("Start merging {} and {}", firstRegionName, secondRegionName);
            admin.mergeRegions(firstRegionName.getBytes(), secondRegionName.getBytes(), true);
            regionsUnderMerge.put(firstRegionName, "");
            regionsUnderMerge.put(secondRegionName, "");
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public int compareTo(MergeTask o) {
        return -1;
    }
}
