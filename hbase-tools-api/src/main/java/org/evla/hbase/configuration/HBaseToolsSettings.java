package org.evla.hbase.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HBaseToolsSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseToolsSettings.class);

    private final CommonSettings commonSettings;
    private final CheckerSettings checkerSettings;

    private final CompactorSettings compactorSettings;
    private final FlusherSettings flusherSettings;
    private final MergerSettings mergerSettings;
    private final DistributorSettings distributorSettings;
    private final SplitterSettings splitterSettings;
    private final TechnicalMetaSettings technicalMetaSettings;
    private final LockSettings lockSettings;

    public HBaseToolsSettings(Properties properties) {
        Properties copy = new Properties();
        copy.putAll(properties);
        this.commonSettings = new CommonSettings(copy);
        this.checkerSettings = new CheckerSettings(copy);
        this.compactorSettings = new CompactorSettings(copy);
        this.flusherSettings = new FlusherSettings(copy);
        this.mergerSettings = new MergerSettings(copy);
        this.distributorSettings = new DistributorSettings(copy);
        this.splitterSettings = new SplitterSettings(copy);
        this.technicalMetaSettings = new TechnicalMetaSettings(copy);
        this.lockSettings = new LockSettings(copy);

        logUnknownProperties(copy);
    }

    public CommonSettings getCommonSettings() {
        return commonSettings;
    }

    public CheckerSettings getCheckerSettings() {
        return checkerSettings;
    }

    public CompactorSettings getCompactorSettings() {
        return compactorSettings;
    }

    public FlusherSettings getFlusherSettings() {
        return flusherSettings;
    }

    public MergerSettings getMergerSettings() {
        return mergerSettings;
    }

    public SplitterSettings getSplitterSettings() {
        return splitterSettings;
    }

    public DistributorSettings getDistributorSettings() {
        return distributorSettings;
    }

    public TechnicalMetaSettings getTechnicalMetaSettings() {
        return technicalMetaSettings;
    }

    public LockSettings getLockSettings() {
        return lockSettings;
    }

    private void logUnknownProperties(Properties properties) {
        for (Object key : properties.keySet()) {
            LOGGER.warn("Configuration {}={} is not apply for hbase-tools", key, properties.get(key));
        }
    }

    public static boolean customizeLogging(Properties properties) {
        Object value = properties.get("customize.logging");
        return value != null && Boolean.parseBoolean(value.toString());
    }
}
