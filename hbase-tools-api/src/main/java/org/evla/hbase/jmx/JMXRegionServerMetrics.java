package org.evla.hbase.jmx;

import org.apache.hadoop.hbase.ServerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.Closeable;
import java.io.IOException;

public class JMXRegionServerMetrics implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JMXRegionServerMetrics.class);

    private ObjectName hbaseRSServer;
    private JMXConnector jmx;
    private MBeanServerConnection jmxConnection = null;

    public JMXRegionServerMetrics(ServerName sn, int jmxPort) {
        initJMXConnection(sn, jmxPort);
    }

    private void initJMXConnection(ServerName sn, int jmxPort) {
        try {
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + sn.getHostname() + ":" + jmxPort + "/jmxrmi");
            jmx = JMXConnectorFactory.connect(url, null);
            jmx.connect();
            hbaseRSServer = new ObjectName("Hadoop:service=HBase,name=RegionServer,sub=Server");
            jmxConnection = jmx.getMBeanServerConnection();
        } catch (Exception e) {
            LOGGER.error("Cant connect to HBase RS {} by jmx. Compaction and flush queues does not taken into account.", sn);
        }
    }

    public Integer getCompactionQueueLength() {
        return getIntegerMBeanValue_int("compactionQueueLength");
    }

    public Integer getFlushQueueLength() {
        return getIntegerMBeanValue_int("flushQueueLength");
    }

    public Double getPercentFilesLocal() {
        return getIntegerMBeanValue_double("percentFilesLocal");
    }

    private Integer getIntegerMBeanValue_int(String attribute) {
        if (jmxConnection != null) {
            try {
                return (Integer) jmxConnection.getAttribute(hbaseRSServer, attribute);
            } catch (Exception e) {
                LOGGER.error("Some Exception occurred: " + e.getMessage(), e);
                jmxConnection = null;
                return 0;
            }
        }
        return 0;
    }

    private Double getIntegerMBeanValue_double(String attribute) {
        if (jmxConnection != null) {
            try {
                return (Double) jmxConnection.getAttribute(hbaseRSServer, attribute);
            } catch (Exception e) {
                LOGGER.error("Some Exception occurred: " + e.getMessage(), e);
                jmxConnection = null;
                return (double) 0;
            }
        }
        return (double) 0;
    }

    @Override
    public void close() {
        if (jmx != null) {
            try {
                jmx.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
