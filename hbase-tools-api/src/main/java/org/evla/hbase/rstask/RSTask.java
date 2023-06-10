package org.evla.hbase.rstask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Callable;

public abstract class RSTask<T> implements Callable<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RSTask.class);

    private Long outerDelay = 0L;

    protected void setOuterDelay(Long outerDelay) {
        this.outerDelay = outerDelay;
    }

    public void waitForExecution() {
        // In most cases we should run any task in sequential way
        if (outerDelay > 0) {
            RSTaskControllerHelper.sleep(new Random().nextInt(outerDelay.intValue()));
        }
    }

    public abstract T executeTask();

    @Override
    public T call() throws Exception {
        waitForExecution();
        return executeTask();
    }
}
