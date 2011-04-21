package io.s4.ft;

import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

public class EagerSerializedStateFetcher implements Runnable {

    private static final int TOKEN_COUNT = Integer.valueOf(System.getProperty(
            "s4.ft.fetcher.token.count", "5"));
    private static final int TOKEN_TIME_MS = Integer.valueOf(System
            .getProperty("s4.ft.fetcher.token.time", "50"));
    SafeKeeper sk;

    private CountDownLatch signalSafeKeeperReady = new CountDownLatch(1);
    private static Logger logger = Logger
            .getLogger(EagerSerializedStateFetcher.class);

    public EagerSerializedStateFetcher(SafeKeeper sk) {
        this.sk = sk;
    }

    public void setSafeKeeperReady() {
        signalSafeKeeperReady.countDown();
    }

    @Override
    public void run() {
        try {
            sk.getReadySignal().await();
        } catch (InterruptedException e1) {
        }

        long startTime = System.currentTimeMillis();
        int tokenCount = TOKEN_COUNT;
        for (SafeKeeperId safeKeeperId : sk.getKeysToRecover()) {

            if (tokenCount == 0) {
                if ((System.currentTimeMillis() - startTime) < (TOKEN_COUNT * TOKEN_TIME_MS)) {
                    try {
                        Thread.sleep(TOKEN_COUNT * TOKEN_TIME_MS
                                - (System.currentTimeMillis() - startTime));
                    } catch (InterruptedException e) {
                        logger.error(e);
                    }
                }
                tokenCount = TOKEN_COUNT;
                startTime = System.currentTimeMillis();
            }

            if (sk.getKeysToRecover().contains(safeKeeperId)) {
                if (!sk.isCached(safeKeeperId)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Fetching state for id: " + safeKeeperId);
                    }

                    byte[] state = sk.fetchSerializedState(safeKeeperId);
                    if (state != null) {
                        sk.cacheSerializedState(safeKeeperId, state);
                        // send an event to recover
                        sk.initiateRecovery(safeKeeperId);
                    }
                    tokenCount--;
                }
            }

        }

    }

}
