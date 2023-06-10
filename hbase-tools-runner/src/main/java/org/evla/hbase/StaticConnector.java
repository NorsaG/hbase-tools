package org.evla.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StaticConnector {
    private static final Logger LOGGER = LoggerFactory.getLogger(StaticConnector.class);

    private static String principal;
    private static String keytab;

    private static String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
    private static String hdfsSite = "/etc/hadoop/conf/hdfs-site.xml";
    private static String coreSite = "/etc/hadoop/conf/core-site.xml";

    private static Configuration config;

    private static Connection connection = null;
    private static Admin admin = null;

    public static void configure(HBaseToolsSettings settings) {
        String coreSite = settings.getCommonSettings().getCoreSiteXml();
        String hdfsSite = settings.getCommonSettings().getHdfsSiteXml();
        String hbaseSite = settings.getCommonSettings().getHBaseSiteXml();
        if (StringUtils.isNotBlank(coreSite)) {
            StaticConnector.setCoreSite(coreSite);
        }
        if (StringUtils.isNotBlank(hdfsSite)) {
            StaticConnector.setHdfsSite(hdfsSite);
        }
        if (StringUtils.isNotBlank(hbaseSite)) {
            StaticConnector.setHbaseSite(hbaseSite);
        }
        StaticConnector.setPrincipal(settings.getCommonSettings().getPrincipal());
        StaticConnector.setKeytab(settings.getCommonSettings().getKeytab());
        StaticConnector.setConfig(StaticConnector.getDefaultConfig());
    }

    public static void setPrincipal(String principal) {
        StaticConnector.principal = principal;
    }

    public static void setKeytab(String keytab) {
        StaticConnector.keytab = keytab;
    }

    public static void setConfig(Configuration config) {
        StaticConnector.config = config;
    }

    public static void setCoreSite(String coreSite) {
        StaticConnector.coreSite = coreSite;
    }

    public static void setHdfsSite(String hdfsSite) {
        StaticConnector.hdfsSite = hdfsSite;
    }

    public static void setHbaseSite(String hbaseSite) {
        StaticConnector.hbaseSite = hbaseSite;
    }

    public static Configuration getDefaultConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path(coreSite));
        conf.addResource(new Path(hdfsSite));
        conf.addResource(new Path(hbaseSite));

        return conf;
    }

    public synchronized static Connection getConnection() {
        checkParameters();

        if (connection != null) {
            return connection;
        }

        try {
            UserGroupInformation.setConfiguration(config);
            UGIExecutor.initUGIExecutor(principal, keytab);
            connection = UGIExecutor.doActionAndReturnResult(() -> {
                Connection con = null;
                try {
                    con = ConnectionFactory.createConnection(config);
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(), e);
                }
                return con;
            });
            UGIExecutor.startRenewTask(60);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
        return connection;
    }

    public synchronized static Admin getAdmin() {
        checkParameters();
        if (admin != null)
            return admin;
        try {
            admin = getConnection().getAdmin();
            return admin;
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public synchronized static void refreshConnection() {
        LOGGER.info("Refresh HBase connections");
        checkParameters();
        connection = null;
        admin = null;

        admin = getAdmin();
    }

    private static void checkParameters() {
        if (StringUtils.isBlank(principal)) {
            throw new RuntimeException("principal is null");
        }
        if (StringUtils.isBlank(keytab)) {
            throw new RuntimeException("keytab is null");
        }
        if (config == null) {
            throw new RuntimeException("config is null");
        }
    }

}
