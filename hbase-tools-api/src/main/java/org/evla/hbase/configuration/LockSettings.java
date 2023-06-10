package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LockSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(LockSettings.class);
    private final Map<String, HBaseToolsProperty> lockSettings = new HashMap<>();

    LockSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Lock settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.LockOptions.getLockSettings()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);

            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            lockSettings.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
    }

    public TableName getLockTable() {
        String name = lockSettings.get(HBaseToolsOptions.LockOptions.LOCK_PROPERTY_TABLE.getName()).getValue();
        if (StringUtils.isBlank(name)) {
            throw new RuntimeException("Lock-table not specified");
        }
        return TableName.valueOf(name);
    }

    public int getDefaultLockTTL() {
        return Integer.parseInt(lockSettings.get(HBaseToolsOptions.LockOptions.LOCK_PROPERTY_TABLE_RECORD_TTL.getName()).getValue());
    }

}
