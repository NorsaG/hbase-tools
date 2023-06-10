package org.evla.hbase.configuration;


import java.util.Arrays;
import java.util.List;

public class HBaseToolsOptions {

    public static class CommonOptions {
        static final HBaseToolsProperty COMMON_PROPERTY_PRINCIPAL = new HBaseToolsProperty("principal", "", "");
        static final HBaseToolsProperty COMMON_PROPERTY_KEYTAB = new HBaseToolsProperty("keytab", "", "");
        static final HBaseToolsProperty COMMON_PROPERTY_CUSTOMIZE_LOGGING = new HBaseToolsProperty("customize.logging", "false", "");

        static final HBaseToolsProperty COMMON_PROPERTY_CORE_SITE = new HBaseToolsProperty("core.site", "/etc/hbase/conf/core-site.xml", "");
        static final HBaseToolsProperty COMMON_PROPERTY_HDFS_SITE = new HBaseToolsProperty("hdfs.site", "/etc/hbase/conf/hdfs-site.xml", "");
        static final HBaseToolsProperty COMMON_PROPERTY_HBASE_SITE = new HBaseToolsProperty("hbase.site", "/etc/hbase/conf/hbase-site.xml", "");

        public static List<HBaseToolsProperty> getCommonOptions() {
            return Arrays.asList(COMMON_PROPERTY_PRINCIPAL, COMMON_PROPERTY_KEYTAB, COMMON_PROPERTY_CUSTOMIZE_LOGGING,
                    COMMON_PROPERTY_CORE_SITE, COMMON_PROPERTY_HDFS_SITE, COMMON_PROPERTY_HBASE_SITE);
        }
    }

    public static class CheckerOptions {
        static final HBaseToolsProperty CHECKER_PROPERTY_ENABLE_CHECK = new HBaseToolsProperty("checker.enable", "false", "");
        static final HBaseToolsProperty CHECKER_PROPERTY_HEALTH_TABLE = new HBaseToolsProperty("checker.health.table", "", "");
        static final HBaseToolsProperty CHECKER_PROPERTY_READ_TABLES_COUNT = new HBaseToolsProperty("checker.read.tables.count", "5", "");
        static final HBaseToolsProperty CHECKER_PROPERTY_CHECK_INTERVAL_SECONDS = new HBaseToolsProperty("checker.check.interval.seconds", "900", "");

        public static List<HBaseToolsProperty> getCheckerOptions() {
            return Arrays.asList(CHECKER_PROPERTY_ENABLE_CHECK, CHECKER_PROPERTY_HEALTH_TABLE, CHECKER_PROPERTY_READ_TABLES_COUNT, CHECKER_PROPERTY_CHECK_INTERVAL_SECONDS);
        }
    }

    public static class ZabbixOptions {
        static final HBaseToolsProperty ZABBIX_PROPERTY = new HBaseToolsProperty("zabbix", "", "");
        static final HBaseToolsProperty ZABBIX_PROPERTY_HOST = new HBaseToolsProperty("zabbix.host", "", "");
        static final HBaseToolsProperty ZABBIX_PROPERTY_PORT = new HBaseToolsProperty("zabbix.port", "", "");
        static final HBaseToolsProperty ZABBIX_PROPERTY_USER = new HBaseToolsProperty("zabbix.user", "", "");
        static final HBaseToolsProperty ZABBIX_PROPERTY_PASSWORD = new HBaseToolsProperty("zabbix.password", "", "");

        public static List<HBaseToolsProperty> getZabbixOptions() {
            return Arrays.asList(ZABBIX_PROPERTY, ZABBIX_PROPERTY_HOST, ZABBIX_PROPERTY_PORT, ZABBIX_PROPERTY_USER, ZABBIX_PROPERTY_PASSWORD);
        }
    }

    public static class CompactorOptions {
        static final HBaseToolsProperty COMPACTOR_PROPERTY_PARALLELISM = new HBaseToolsProperty("compactor.parallel.compactions", "2", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_STATUS_DELAY = new HBaseToolsProperty("compactor.status.delay", "5000", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_ADDITIONAL_DELAY = new HBaseToolsProperty("compactor.addition.delay", "10000", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_ACTUALIZE_DELAY = new HBaseToolsProperty("compactor.actualize.delay", "1800000", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_STORE_SIZE_MB = new HBaseToolsProperty("compactor.store.size.mb", "100", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_JMX_PORTS = new HBaseToolsProperty("compactor.jmx.ports", "", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_MAX_COMPACTIONS_BORDER = new HBaseToolsProperty("compactor.max.compactions.border", "11", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_MAX_FLUSHES_BORDER = new HBaseToolsProperty("compactor.max.flushes.border", "31", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_SORTING_ENABLE = new HBaseToolsProperty("compactor.sorting.enable", "false", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_WEIGHT_BORDER = new HBaseToolsProperty("compactor.border.weight", "15", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_RECALCULATE_REGION_COUNT = new HBaseToolsProperty("compactor.recalculate.region.count", "15", "");
        static final HBaseToolsProperty COMPACTOR_PROPERTY_RS_REFRESH_DELAY = new HBaseToolsProperty("compactor.rs.refresh.delay", "90000", "");

        public static List<HBaseToolsProperty> getCompactorOptions() {
            return Arrays.asList(COMPACTOR_PROPERTY_PARALLELISM, COMPACTOR_PROPERTY_STATUS_DELAY, COMPACTOR_PROPERTY_ADDITIONAL_DELAY,
                    COMPACTOR_PROPERTY_ACTUALIZE_DELAY, COMPACTOR_PROPERTY_STORE_SIZE_MB, COMPACTOR_PROPERTY_JMX_PORTS,
                    COMPACTOR_PROPERTY_MAX_COMPACTIONS_BORDER, COMPACTOR_PROPERTY_MAX_FLUSHES_BORDER, COMPACTOR_PROPERTY_SORTING_ENABLE,
                    COMPACTOR_PROPERTY_WEIGHT_BORDER, COMPACTOR_PROPERTY_RECALCULATE_REGION_COUNT, COMPACTOR_PROPERTY_RS_REFRESH_DELAY
            );
        }
    }

    public static class FlusherOptions {
        static final HBaseToolsProperty FLUSHER_PROPERTY_THREAD_COUNT = new HBaseToolsProperty("flusher.thread.count", "3", "");
        static final HBaseToolsProperty FLUSHER_PROPERTY_MEMSTORE_BORDER_MB = new HBaseToolsProperty("flusher.memstore.memory.border.mb", "1", "");

        public static List<HBaseToolsProperty> getFlusherOptions() {
            return Arrays.asList(FLUSHER_PROPERTY_THREAD_COUNT, FLUSHER_PROPERTY_MEMSTORE_BORDER_MB);
        }
    }

    public static class DistributorOptions {
        static final HBaseToolsProperty DISTRIBUTOR_PROPERTY_REGION_WEIGHT_BORDER = new HBaseToolsProperty("distributor.region.weight.border", "5", "");
        static final HBaseToolsProperty DISTRIBUTOR_PROPERTY_THREADS = new HBaseToolsProperty("distributor.threads", "3", "");
        static final HBaseToolsProperty DISTRIBUTOR_PROPERTY_RIT_TIMEOUT = new HBaseToolsProperty("distributor.rit.timeout", "3000", "");
        static final HBaseToolsProperty DISTRIBUTOR_PROPERTY_COMPACT = new HBaseToolsProperty("distributor.compact", "true", "");
        static final HBaseToolsProperty DISTRIBUTOR_PROPERTY_RECALCULATE_TABLE_COUNT = new HBaseToolsProperty("distributor.recalculate.table.count", "15", "");

        public static List<HBaseToolsProperty> getDistributorOptions() {
            return Arrays.asList(DISTRIBUTOR_PROPERTY_REGION_WEIGHT_BORDER, DISTRIBUTOR_PROPERTY_THREADS, DISTRIBUTOR_PROPERTY_RIT_TIMEOUT, DISTRIBUTOR_PROPERTY_COMPACT, DISTRIBUTOR_PROPERTY_RECALCULATE_TABLE_COUNT);
        }
    }

    public static class MergerOptions {
        static final HBaseToolsProperty MERGER_PROPERTY_QUALITY = new HBaseToolsProperty("merger.quality", "по-взрослому", "");
        static final HBaseToolsProperty MERGER_PROPERTY_REGIONS_MIN_STOREFILE_SIZE_MB = new HBaseToolsProperty("merger.regions.min-storefile-size-mb", "64", "");
        static final HBaseToolsProperty MERGER_PROPERTY_REGIONS_MAX_STOREFILE_SIZE_MB = new HBaseToolsProperty("merger.regions.max-storefile-size-mb", "6124", "");
        static final HBaseToolsProperty MERGER_PROPERTY_REGIONS_MAX_MERGED_STOREFILE_SIZE_MB = new HBaseToolsProperty("merger.regions.max-merged-storefile-size-mb", "8192", "");
        static final HBaseToolsProperty MERGER_PROPERTY_CHECK_SNAPSHOTS_EXISTS = new HBaseToolsProperty("merger.check.snapshot.exists", "true", "");

        public static List<HBaseToolsProperty> getMergerOptions() {
            return Arrays.asList(MERGER_PROPERTY_QUALITY, MERGER_PROPERTY_REGIONS_MIN_STOREFILE_SIZE_MB, MERGER_PROPERTY_REGIONS_MAX_STOREFILE_SIZE_MB,
                    MERGER_PROPERTY_REGIONS_MAX_MERGED_STOREFILE_SIZE_MB, MERGER_PROPERTY_CHECK_SNAPSHOTS_EXISTS);
        }
    }

    public static class SplitterOptions {
        static final HBaseToolsProperty SPLITTER_PROPERTY_SPLIT_MULTIPLIER = new HBaseToolsProperty("splitter.split.multiplier", "1.1", "");
        static final HBaseToolsProperty SPLITTER_PROPERTY_DIVIDE_MULTIPLIER = new HBaseToolsProperty("splitter.divide.multiplier", "1.1", "");
        static final HBaseToolsProperty SPLITTER_PROPERTY_DISTRIBUTE = new HBaseToolsProperty("splitter.distribute", "true", "");

        public static List<HBaseToolsProperty> getSplitterOptions() {
            return Arrays.asList(SPLITTER_PROPERTY_SPLIT_MULTIPLIER, SPLITTER_PROPERTY_DIVIDE_MULTIPLIER, SPLITTER_PROPERTY_DISTRIBUTE);
        }
    }

    public static class TechnicalMetaOptions {
        static final HBaseToolsProperty TECHNICAL_META_PROPERTY_ENABLE = new HBaseToolsProperty("technical.meta.enable", "false", "");
        static final HBaseToolsProperty TECHNICAL_META_PROPERTY_TABLE = new HBaseToolsProperty("technical.meta.table", "HBASE_TOOLS_TECHNICAL_META", "");
        static final HBaseToolsProperty TECHNICAL_META_PROPERTY_TABLE_TTL = new HBaseToolsProperty("technical.meta.table.ttl.sec", "2592000", "");
        static final HBaseToolsProperty TECHNICAL_META_PROPERTY_TABLE_RECORD_TTL = new HBaseToolsProperty("technical.meta.table.record.ttl.millis", "604800000", "");
        static final HBaseToolsProperty TECHNICAL_META_PROPERTY_SCAN_PERIOD = new HBaseToolsProperty("technical.meta.scan.period.sec", "3600", "");

        public static List<HBaseToolsProperty> getTechnicalMetaSettings() {
            return Arrays.asList(TECHNICAL_META_PROPERTY_ENABLE, TECHNICAL_META_PROPERTY_TABLE, TECHNICAL_META_PROPERTY_TABLE_TTL, TECHNICAL_META_PROPERTY_TABLE_RECORD_TTL, TECHNICAL_META_PROPERTY_SCAN_PERIOD);
        }
    }

    public static class LockOptions {
        static final HBaseToolsProperty LOCK_PROPERTY_TABLE = new HBaseToolsProperty("lock.table", "", "");
        static final HBaseToolsProperty LOCK_PROPERTY_TABLE_RECORD_TTL = new HBaseToolsProperty("lock.table.record.ttl.millis", "4000000", "");

        public static List<HBaseToolsProperty> getLockSettings() {
            return Arrays.asList(LOCK_PROPERTY_TABLE, LOCK_PROPERTY_TABLE_RECORD_TTL);
        }
    }
}
