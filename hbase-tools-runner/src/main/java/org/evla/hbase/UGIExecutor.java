package org.evla.hbase;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class UGIExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(UGIExecutor.class);

    private static UserGroupInformation ugi = null;

    private static ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("UGIExecutor-thread-%d").build());

    public static void initUGIExecutor(String principal, String keytab) {
        try {
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);

        } catch (IOException e) {
            LOGGER.error("Cant get actual UserGroupInformation. Error: " + e.getMessage(), e);
            try {
                ugi = UserGroupInformation.getCurrentUser();
            } catch (IOException e1) {
                LOGGER.error("Cant get current UserGroupInformation. Error: " + e.getMessage(), e);
            }
        }
    }

    public static void loginWithCreds(String principal, String keytab) {
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (IOException e) {
            LOGGER.error("Cant log in with specified principal and keytab. Error: " + e.getMessage(), e);
        }
    }

    private static void checkInitialization() {
        Objects.requireNonNull(ugi, "UserGroupInformation inside UGIExecutor not initialized!");
    }

    public static <T> T doActionAndReturnResult(PrivilegedExceptionAction<T> action) {
        checkInitialization();
        try {
            return ugi.doAs(action);
        } catch (Exception e) {
            LOGGER.error("Cant execute action in secured context. Try to execute without context. Error: " + e.getMessage(), e);
            try {
                return action.run();
            } catch (Exception e1) {
                LOGGER.error("Cant execute action in unsecured context: " + e.getMessage(), e);
                throw new RuntimeException("Cant execute action: " + action.toString(), e);
            }
        }
    }

    public static void startRenewTask(long repeatTime) {
        checkInitialization();
        startRenewTask(ugi, repeatTime);
    }

    static void startRenewTask(UserGroupInformation information, long repeatTime) {
        executorService.scheduleWithFixedDelay(() -> {
            try {
                LOGGER.debug("Check TGT and relogin if needed.");
                information.checkTGTAndReloginFromKeytab();
            } catch (IOException e) {
                LOGGER.error("Got exception while trying to refresh credentials: " + e.getMessage(), e);
                throw new RuntimeException("Got exception while trying to refresh credentials", e);
            }
        }, repeatTime, repeatTime, TimeUnit.MINUTES);
    }

    static void stopRenew() {
        executorService.shutdown();
    }
}