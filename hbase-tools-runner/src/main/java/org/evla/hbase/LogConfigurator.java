package org.evla.hbase;

import org.apache.log4j.PropertyConfigurator;

import java.util.Properties;

public class LogConfigurator {
    static void configureLogging(Tool tool) {
        int pid = Tool.getPID();

        Properties properties = new Properties();
        properties.setProperty("log4j.rootLogger", "ERROR");
        properties.setProperty("log4j.logger.org.apache.zookeeper", "ERROR");
        properties.setProperty("log4j.logger.zookeeper", "ERROR");

        properties.setProperty("log4j.logger.org.evla.hbase", "INFO, HBASE-TOOLS, stdout");

        properties.setProperty("log4j.appender.stdout", "org.apache.log4j.ConsoleAppender");
        properties.setProperty("log4j.appender.stdout.Target", "System.out");
        properties.setProperty("log4j.appender.stdout.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.stdout.layout.ConversionPattern", "[%d{yyyy-MM-dd HH:mm:ss.SSS}] [%t] [%-4p] %c{1}:%L - %m%n");

        properties.setProperty("log4j.appender.HBASE-TOOLS", "org.apache.log4j.rolling.RollingFileAppender");
//        properties.setProperty("log4j.appender.HBASE-TOOLS.File",   "./hbase-tools.log");
        properties.setProperty("log4j.appender.HBASE-TOOLS.rollingPolicy", "org.apache.log4j.rolling.FixedWindowRollingPolicy");
        properties.setProperty("log4j.appender.HBASE-TOOLS.rollingPolicy.activeFileName", "./logs/hbase-tools-" + tool.getName() + "-" + pid + ".log");
        properties.setProperty("log4j.appender.HBASE-TOOLS.rollingPolicy.fileNamePattern", "./logs/hbase-tools-" + tool.getName() + "-" + pid + "-%i.log.gz");
        properties.setProperty("log4j.appender.HBASE-TOOLS.triggeringPolicy", "org.apache.log4j.rolling.SizeBasedTriggeringPolicy");
        properties.setProperty("log4j.appender.HBASE-TOOLS.triggeringPolicy.MaxFileSize", "524288000");

//        properties.setProperty("log4j.appender.HBASE-TOOLS.MaxFileSize", "100MB");
//        properties.setProperty("log4j.appender.HBASE-TOOLS.MaxBackupIndex", "10");
        properties.setProperty("log4j.appender.HBASE-TOOLS.layout", "org.apache.log4j.PatternLayout");
        properties.setProperty("log4j.appender.HBASE-TOOLS.layout.ConversionPattern", "[%d{yyyy-MM-dd HH:mm:ss.SSS}] [%t] [%-4p] %c{1}:%L - %m%n");

        PropertyConfigurator.configure(properties);
    }
}
