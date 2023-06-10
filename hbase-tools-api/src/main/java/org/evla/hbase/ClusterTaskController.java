package org.evla.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.evla.hbase.rstask.RSTaskController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Supplier;

public abstract class ClusterTaskController<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterTaskController.class);
    protected List<ServerName> servers = null;

    public void setServers(List<ServerName> servers) {
        this.servers = servers;
    }

    public void start() {
        LOGGER.info("Start {}", type());
        Map<ServerName, RSTaskController<T>> controllers = getControllers();
        LOGGER.info("Init tasks for {}", type());
        initTasks(controllers);

        List<Callable<T>> actions = new ArrayList<>();
        controllers.forEach((key, value) -> actions.add(() -> {
            T result = null;
            if (servers == null || servers.contains(key)) {
                result = getRSThreadRunner(key, value).get();
            }
            return result;
        }));
        ExecutorService clusterController = null;
        try {
            clusterController = Executors.newFixedThreadPool(controllers.size(), new ThreadFactoryBuilder().setDaemon(true).setNameFormat("controller-%d").build());
            clusterController.invokeAll(actions);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        } finally {
            if (clusterController != null) {
                clusterController.shutdown();
            }
        }
        LOGGER.info("Stop {}", type());
    }

    public abstract void initTasks(Map<ServerName, RSTaskController<T>> controllers);

    public abstract Map<ServerName, RSTaskController<T>> getControllers();

    public abstract Supplier<T> getRSThreadRunner(ServerName sn, RSTaskController<T> controller);

    public abstract String type();

}
