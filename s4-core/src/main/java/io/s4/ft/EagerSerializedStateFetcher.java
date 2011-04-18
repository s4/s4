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
        // FIXME log
        System.out.println("STARTING EAGER FETCHING THREAD");
        try {
            sk.getReadySignal().await();
        } catch (InterruptedException e1) {
        }
        Set<SafeKeeperId> storedKeys = sk.getStateStorage().fetchStoredKeys();
        int nodeCount = sk.getLoopbackDispatcher().getEventEmitter()
                .getNodeCount();
        // required wait until nodes are available
        while (nodeCount == 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            nodeCount = sk.getLoopbackDispatcher().getEventEmitter()
                    .getNodeCount();
        }

        for (SafeKeeperId key : storedKeys) {
            // validate ids through hash function
            if (Integer.valueOf(sk.getPartitionId()).equals(
                    (sk.getHasher().hash(key.getKey()) % nodeCount))) {
                sk.getKeysToRecover().add(key);
            }
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
