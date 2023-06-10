package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MergerSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsSettings.class);
    private final Map<String, HBaseToolsProperty> mergerSettings = new HashMap<>();

    MergerSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Merger settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.MergerOptions.getMergerOptions()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);
            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            mergerSettings.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
    }

    public String getMergerQuality() {
        return mergerSettings.get(HBaseToolsOptions.MergerOptions.MERGER_PROPERTY_QUALITY.getName()).getValue();
    }

    public int getMergerMinStoreFileSize() {
        return Integer.parseInt(mergerSettings.get(HBaseToolsOptions.MergerOptions.MERGER_PROPERTY_REGIONS_MIN_STOREFILE_SIZE_MB.getName()).getValue());
    }

    public int getMergerMaxStoreFileSize() {
        return Integer.parseInt(mergerSettings.get(HBaseToolsOptions.MergerOptions.MERGER_PROPERTY_REGIONS_MAX_STOREFILE_SIZE_MB.getName()).getValue());
    }

    public int getMergerMaxMergedStoreFileSize() {
        return Integer.parseInt(mergerSettings.get(HBaseToolsOptions.MergerOptions.MERGER_PROPERTY_REGIONS_MAX_MERGED_STOREFILE_SIZE_MB.getName()).getValue());
    }

    public boolean isCheckSnapshot() {
        return Boolean.parseBoolean(mergerSettings.get(HBaseToolsOptions.MergerOptions.MERGER_PROPERTY_CHECK_SNAPSHOTS_EXISTS.getName()).getValue());
    }

}
