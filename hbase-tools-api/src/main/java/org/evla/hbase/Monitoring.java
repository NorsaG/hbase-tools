package org.evla.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class Monitoring implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Monitoring.class);

    private final ScheduledExecutorService monitor;
    private final List<Consumer<Logger>> customMetrics = new ArrayList<>();

    public Monitoring(String monitorName) {
        Objects.requireNonNull(monitorName);

        monitor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("monitor-" + monitorName).build()
        );
    }

    public void registerSingleMonitor(Consumer<Logger> c) {
        customMetrics.add(c);
    }

    public void startMonitoring(TimeUnit timeUnit, int delay) {
        monitor.scheduleWithFixedDelay(() -> customMetrics.forEach(loggerConsumer -> loggerConsumer.accept(LOGGER)), delay, delay, timeUnit);
    }

    public void startMonitoring() {
        startMonitoring(TimeUnit.SECONDS, 3);
    }

    public void stopMonitor() {
        monitor.shutdown();
    }

    @Override
    public void close() {
        stopMonitor();
    }
}
