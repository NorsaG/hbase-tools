package org.evla.hbase.rstask;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RSTaskController<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RSTaskController.class);

    private final ServerName serverName;
    private final ExecutorService service;
    private final Queue<RSTask<T>> taskQueue = new LinkedList<>();

    RSTaskController(ServerName serverName, int threads) {
        this.serverName = serverName;
        this.service = Executors.newFixedThreadPool(threads, new ThreadFactoryBuilder().setNameFormat(serverName.getHostAndPort() + "-%d").build());
    }

    public void processTasks() {
        try {
            service.invokeAll(taskQueue);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public void addTask(RSTask<T> task) {
        taskQueue.add(task);
    }

    public void addAllTask(List<? extends RSTask<T>> tasks) {
        taskQueue.addAll(tasks);
    }

    public int getTasksCount() {
        return taskQueue.size();
    }

    public ServerName getServerName() {
        return serverName;
    }

}
