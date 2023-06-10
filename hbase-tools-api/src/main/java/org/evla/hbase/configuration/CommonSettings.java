package org.evla.hbase.configuration;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CommonSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsSettings.class);

    private final Map<String, HBaseToolsProperty> commonProperties = new HashMap<>();

    CommonSettings(Properties properties) {
        initPropertiesMap(properties);
    }

    private void initPropertiesMap(Properties loadProperties) {
        StringJoiner sj = new StringJoiner("\n\t");
        sj.add("Common settings:");
        Set<String> tmp = new HashSet<>();
        for (HBaseToolsProperty option : HBaseToolsOptions.CommonOptions.getCommonOptions()) {
            String name = option.getName();
            String value = loadProperties.getProperty(name);
            boolean isExists = false;
            if (StringUtils.isNotBlank(value)) {
                isExists = true;
                option.setValue(value);
                tmp.add(name);
            }
            sj.add(name + "=" + option.getValue() + (!isExists ? " (default)" : ""));
            commonProperties.put(name, option);
        }
        LOGGER.info(sj.toString());
        tmp.forEach(loadProperties::remove);
    }

    public String getPrincipal() {
        return commonProperties.get(HBaseToolsOptions.CommonOptions.COMMON_PROPERTY_PRINCIPAL.getName()).getValue();
    }

    public String getKeytab() {
        return commonProperties.get(HBaseToolsOptions.CommonOptions.COMMON_PROPERTY_KEYTAB.getName()).getValue();
    }

    public boolean isCustomizeLogging() {
        return Boolean.parseBoolean(commonProperties.get(HBaseToolsOptions.CommonOptions.COMMON_PROPERTY_CUSTOMIZE_LOGGING.getName()).getValue());
    }

    public String getCoreSiteXml() {
        return commonProperties.get(HBaseToolsOptions.CommonOptions.COMMON_PROPERTY_CORE_SITE.getName()).getValue();
    }

    public String getHdfsSiteXml() {
        return commonProperties.get(HBaseToolsOptions.CommonOptions.COMMON_PROPERTY_HDFS_SITE.getName()).getValue();
    }

    public String getHBaseSiteXml() {
        return commonProperties.get(HBaseToolsOptions.CommonOptions.COMMON_PROPERTY_HBASE_SITE.getName()).getValue();
    }

}
