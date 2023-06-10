package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TechnicalMetaSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(TechnicalMetaSettings.class);
    private final Map<String, HBaseToolsProperty> technicalSettings = new HashMap<>();

    TechnicalMetaSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Technical meta settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.TechnicalMetaOptions.getTechnicalMetaSettings()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);

            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            technicalSettings.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
    }

    public TableName getTechnicalMetaTable() {
        return TableName.valueOf(technicalSettings.get(HBaseToolsOptions.TechnicalMetaOptions.TECHNICAL_META_PROPERTY_TABLE.getName()).getValue());
    }

    public int getTechnicalMetaScanPeriod() {
        return Integer.parseInt(technicalSettings.get(HBaseToolsOptions.TechnicalMetaOptions.TECHNICAL_META_PROPERTY_SCAN_PERIOD.getName()).getValue());
    }

    public int getTechnicalMetaTableTTL() {
        return Integer.parseInt(technicalSettings.get(HBaseToolsOptions.TechnicalMetaOptions.TECHNICAL_META_PROPERTY_TABLE_TTL.getName()).getValue());
    }

    public long getTechnicalMetaTableRecordTTL() {
        return Integer.parseInt(technicalSettings.get(HBaseToolsOptions.TechnicalMetaOptions.TECHNICAL_META_PROPERTY_TABLE_RECORD_TTL.getName()).getValue());
    }

    public boolean isTechnicalMetaEnable() {
        return Boolean.parseBoolean(technicalSettings.get(HBaseToolsOptions.TechnicalMetaOptions.TECHNICAL_META_PROPERTY_ENABLE.getName()).getValue());
    }
}
