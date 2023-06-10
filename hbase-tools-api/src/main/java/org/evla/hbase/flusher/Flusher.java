package org.evla.hbase.flusher;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.evla.hbase.configuration.HBaseToolsSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class Flusher {
    private static final Logger LOGGER = LoggerFactory.getLogger(Flusher.class);

    private final Admin admin;
    private final HBaseToolsSettings settings;
    private List<ServerName> servers;

    public Flusher(Admin admin, HBaseToolsSettings settings) {
        this.admin = admin;
        this.settings = settings;
        initServers(admin);
    }

    private void initServers(Admin admin) {
        try {
            this.servers = new ArrayList<>(admin.getClusterStatus().getServers());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void start(String serversStr) {
        List<String> specServers = Arrays.asList(serversStr.split(","));
        List<ServerName> specServersList = new ArrayList<>();
        specServers.forEach(s -> {
            for (ServerName sn : servers) {
                if (sn.getHostname().startsWith(s)) {
                    specServersList.add(sn);
                }
            }
        });
        this.servers = specServersList;
        start();
    }

    void start() {
        FlushController flushController = new FlushController(admin, settings);
        if (servers != null) {
            flushController.setServers(servers);
        }
        flushController.start();
    }

}
