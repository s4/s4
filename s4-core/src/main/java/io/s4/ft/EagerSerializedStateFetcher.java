package io.s4.ft;

import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

public class EagerSerializedStateFetcher implements Runnable {

    private static final int TOKEN_COUNT = Integer.valueOf(System.getProperty(
            "s4.ft.fetcher.token.count", "5"));
    private static final int TOKEN_TIME_MS = Integer.valueOf(System
            .getProperty("s4.ft.fetcher.token.time", "50"));
    SafeKeeper sk;

    private static Logger LOG = Logger
            .getLogger(EagerSerializedStateFetcher.class);

    public EagerSerializedStateFetcher(SafeKeeper sk) {
        this.sk = sk;
    }

    @Override
    public void run() {
        // FIXME log
        System.out.println("STARTING EAGER FETCHING THREAD");
        Set<SafeKeeperId> storedKeys = sk.getStateStorage().fetchStoredKeys();
        for (SafeKeeperId key : storedKeys) {
            // TODO validate ids through hash function?
        }
        sk.getKeysToRecover().addAll(storedKeys);

        long startTime = System.currentTimeMillis();
        int tokenCount = TOKEN_COUNT;
        for (SafeKeeperId safeKeeperId : storedKeys) {

            if (tokenCount == 0) {
                if ((System.currentTimeMillis() - startTime) < (TOKEN_COUNT * TOKEN_TIME_MS)) {
                    try {
                        Thread.sleep(TOKEN_COUNT * TOKEN_TIME_MS
                                - (System.currentTimeMillis() - startTime));
                    } catch (InterruptedException e) {
                        LOG.error(e);
                    }
                }
                tokenCount = TOKEN_COUNT;
                startTime = System.currentTimeMillis();
            }

            if (sk.getKeysToRecover().contains(safeKeeperId)) {
                if (!sk.isCached(safeKeeperId)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Fetching state for id: " + safeKeeperId);
                    }

                    byte[] state = sk.fetchSerializedState(safeKeeperId);
                    if (state != null) {
                        sk.cacheSerializedState(safeKeeperId, state);
                    }
                    tokenCount--;
                }
            }

        }

    }

}
