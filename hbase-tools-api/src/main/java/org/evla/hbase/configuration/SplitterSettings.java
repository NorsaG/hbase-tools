package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SplitterSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsSettings.class);
    private final Map<String, HBaseToolsProperty> splitterSettings = new HashMap<>();

    SplitterSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Splitter settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.SplitterOptions.getSplitterOptions()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);

            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            splitterSettings.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
    }

    public double getSplitMultiplier() {
        return Double.parseDouble(splitterSettings.get(HBaseToolsOptions.SplitterOptions.SPLITTER_PROPERTY_SPLIT_MULTIPLIER.getName()).getValue());
    }

    public double getSplitDivideMultiplier() {
        return Double.parseDouble(splitterSettings.get(HBaseToolsOptions.SplitterOptions.SPLITTER_PROPERTY_DIVIDE_MULTIPLIER.getName()).getValue());
    }

    public boolean isDistributeAfterSplit() {
        return Boolean.parseBoolean(splitterSettings.get(HBaseToolsOptions.SplitterOptions.SPLITTER_PROPERTY_DISTRIBUTE.getName()).getValue());
    }
}
