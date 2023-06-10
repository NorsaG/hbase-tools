package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CheckerSettings {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsSettings.class);
    private final Map<String, HBaseToolsProperty> checkerSettings = new HashMap<>();

    CheckerSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Checker settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.CheckerOptions.getCheckerOptions()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);
            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            checkerSettings.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
    }

    public String getHealthTable() {
        return checkerSettings.get(HBaseToolsOptions.CheckerOptions.CHECKER_PROPERTY_HEALTH_TABLE.getName()).getValue();
    }

    public int getCheckTablesCount() {
        return Integer.parseInt(checkerSettings.get(HBaseToolsOptions.CheckerOptions.CHECKER_PROPERTY_READ_TABLES_COUNT.getName()).getValue());
    }

    public int getCheckInterval() {
        return Integer.parseInt(checkerSettings.get(HBaseToolsOptions.CheckerOptions.CHECKER_PROPERTY_CHECK_INTERVAL_SECONDS.getName()).getValue());
    }

    public boolean isCheckerEnable() {
        return Boolean.parseBoolean(checkerSettings.get(HBaseToolsOptions.CheckerOptions.CHECKER_PROPERTY_ENABLE_CHECK.getName()).getValue());
    }
}
