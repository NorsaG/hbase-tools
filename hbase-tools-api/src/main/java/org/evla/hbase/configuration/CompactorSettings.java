package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CompactorSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsSettings.class);

    private final Map<String, HBaseToolsProperty> compactorProperties = new HashMap<>();

    private Map<Integer, Integer> jmxPorts;

    CompactorSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Compactor settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.CompactorOptions.getCompactorOptions()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);
            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            compactorProperties.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
        setJmx();
    }

    private void setJmx() {
        HBaseToolsProperty jmxSettings = compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_JMX_PORTS.getName());
        String line = jmxSettings.getValue();
        if (StringUtils.isNotBlank(line)) {
            jmxPorts = ports(line);
        } else {
            jmxPorts = Collections.emptyMap();
        }
    }

    private Map<Integer, Integer> ports(String jmxSettings) {
        String[] parts = jmxSettings.split(",");
        Map<Integer, Integer> map = new HashMap<>(parts.length);
        for (String p : parts) {
            int index = p.indexOf(":");
            map.put(Integer.parseInt(p.substring(0, index)), Integer.parseInt(p.substring(index + 1)));
        }
        return map;
    }

    public int getParallelCompaction() {
        return Integer.parseInt(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_PARALLELISM.getName()).getValue());
    }

    public long getStatusDelay() {
        return Long.parseLong(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_STATUS_DELAY.getName()).getValue());
    }

    public long getAdditionDelay() {
        return Long.parseLong(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_ADDITIONAL_DELAY.getName()).getValue());
    }

    public long getActualizeTimeout() {
        return Long.parseLong(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_ACTUALIZE_DELAY.getName()).getValue());
    }

    public long getStoreSizeMb() {
        return Long.parseLong(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_STORE_SIZE_MB.getName()).getValue());
    }

    public Map<Integer, Integer> getJmxPorts() {
        return jmxPorts;
    }

    public Integer getJmxPort(int serverPort) {
        return jmxPorts.getOrDefault(serverPort, -1);
    }

    public int getMaxCompactionsBorder() {
        return Integer.parseInt(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_MAX_COMPACTIONS_BORDER.getName()).getValue());
    }

    public int getMaxFlushesBorder() {
        return Integer.parseInt(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_MAX_FLUSHES_BORDER.getName()).getValue());
    }

    public boolean isNeedSorting() {
        return Boolean.parseBoolean(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_STATUS_DELAY.getName()).getValue());
    }

    public int getApproximateBorderWeight() {
        return Integer.parseInt(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_WEIGHT_BORDER.getName()).getValue());
    }

    public int getRecalculateRegionCount() {
        return Integer.parseInt(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_RECALCULATE_REGION_COUNT.getName()).getValue());
    }

    public long getRefreshDelay() {
        return Long.parseLong(compactorProperties.get(HBaseToolsOptions.CompactorOptions.COMPACTOR_PROPERTY_RS_REFRESH_DELAY.getName()).getValue());
    }
}

