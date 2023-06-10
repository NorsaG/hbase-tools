package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class FlusherSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsSettings.class);
    private final Map<String, HBaseToolsProperty> flusherSettings = new HashMap<>();

    FlusherSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Flusher settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.FlusherOptions.getFlusherOptions()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);
            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            flusherSettings.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
    }

    public int getFlusherThreads() {
        return Integer.parseInt(flusherSettings.get(HBaseToolsOptions.FlusherOptions.FLUSHER_PROPERTY_THREAD_COUNT.getName()).getValue());
    }

    public int getFlusherMemstoreBorder() {
        return Integer.parseInt(flusherSettings.get(HBaseToolsOptions.FlusherOptions.FLUSHER_PROPERTY_MEMSTORE_BORDER_MB.getName()).getValue());
    }

}
