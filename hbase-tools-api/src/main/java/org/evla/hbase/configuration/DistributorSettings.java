package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DistributorSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsSettings.class);
    private final Map<String, HBaseToolsProperty> distributorSettings = new HashMap<>();

    DistributorSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Distributor settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.DistributorOptions.getDistributorOptions()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);
            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            distributorSettings.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
    }

    public int getRegionWeightBorder() {
        return Integer.parseInt(distributorSettings.get(HBaseToolsOptions.DistributorOptions.DISTRIBUTOR_PROPERTY_REGION_WEIGHT_BORDER.getName()).getValue());
    }

    public int getDistributorThreads() {
        return Integer.parseInt(distributorSettings.get(HBaseToolsOptions.DistributorOptions.DISTRIBUTOR_PROPERTY_THREADS.getName()).getValue());
    }

    public int getDistributorRITTimeout() {
        return Integer.parseInt(distributorSettings.get(HBaseToolsOptions.DistributorOptions.DISTRIBUTOR_PROPERTY_RIT_TIMEOUT.getName()).getValue());
    }

    public boolean isCompactAfterDistribute() {
        return Boolean.parseBoolean(distributorSettings.get(HBaseToolsOptions.DistributorOptions.DISTRIBUTOR_PROPERTY_COMPACT.getName()).getValue());
    }

    public int getRecalculateTableCount() {
        return Integer.parseInt(distributorSettings.get(HBaseToolsOptions.DistributorOptions.DISTRIBUTOR_PROPERTY_RECALCULATE_TABLE_COUNT.getName()).getValue());
    }

}
