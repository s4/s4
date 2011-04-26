package io.s4.ft;

import io.s4.dispatcher.Dispatcher;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.dispatcher.partitioner.Hasher;
import io.s4.dispatcher.partitioner.KeyInfo;
import io.s4.processor.AbstractPE;
import io.s4.serialize.SerializerDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

public class SafeKeeper {

    public enum StorageResultCode {
        SUCCESS, FAILURE
    }

    private static Logger logger = Logger.getLogger(SafeKeeper.class);
    private ConcurrentMap<SafeKeeperId, byte[]> serializedStateCache = new ConcurrentHashMap<SafeKeeperId, byte[]>(
            16, 0.75f, 2);
    private Set<SafeKeeperId> keysToRecover = Collections
            .newSetFromMap(new ConcurrentHashMap<SafeKeeperId, Boolean>(16,
                    0.75f, 2));
    private Thread eagerFetchingThread;
    private StateStorage stateStorage;
    private Dispatcher loopbackDispatcher;
    private SerializerDeserializer serializer;
    private boolean eagerRecovery = false;
    private Hasher hasher;
    // FIXME currently using partition id to identify current node
    private String partitionId;
    // monitor field injection through a latch
    private CountDownLatch signalReady = new CountDownLatch(3);
    private CountDownLatch signalKeysLoaded = new CountDownLatch(1);

    public SafeKeeper() {
    }

    public void init() {
        
        Thread keysLoaderThread = new Thread(new KeysLoader(this));
        keysLoaderThread.start();
        
        if (eagerRecovery) {
            eagerFetchingThread = new Thread(new EagerSerializedStateFetcher(
                    this), "EagerSerializedStateLoader");
            eagerFetchingThread.start();
        }
        
    }
    
    private static class KeysLoader implements Runnable {
        SafeKeeper safeKeeper;
        
        public KeysLoader(SafeKeeper safeKeeper) {
            this.safeKeeper = safeKeeper;
        }
        
        public void run() {
            try {
                safeKeeper.getReadySignal().await();
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            Set<SafeKeeperId> storedKeys = safeKeeper.getStateStorage().fetchStoredKeys();
            int nodeCount = safeKeeper.getLoopbackDispatcher().getEventEmitter()
                    .getNodeCount();
            // required wait until nodes are available
            // NOTE: this only works for a config with a static number of nodes
            while (nodeCount == 0) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                nodeCount = safeKeeper.getLoopbackDispatcher().getEventEmitter()
                        .getNodeCount();
            }

            for (SafeKeeperId key : storedKeys) {
                // validate ids through hash function
                if (Long.valueOf(safeKeeper.getPartitionId()).equals(Long.valueOf(
                        (safeKeeper.getHasher().hash(key.getKey()) % nodeCount)))) {
                    safeKeeper.getKeysToRecover().add(key);
                }
            }
            safeKeeper.signalKeysLoaded.countDown();

        }
    }

    public void saveState(SafeKeeperId key, byte[] state) {
        stateStorage.saveState(key, state, new StorageCallBackLogger());
    }

    public byte[] fetchSerializedState(SafeKeeperId key) {
        try {
            signalKeysLoaded.await();
        } catch (InterruptedException ignored) {
        }
        byte[] result = null;
        if(keysToRecover.contains(key)) {
            if (serializedStateCache.containsKey(key)) {
                result = serializedStateCache.remove(key);
            } else {
                result = stateStorage.fetchState(key);
            }
            keysToRecover.remove(key);
        }
        return result;
    }

    // TODO externalize
    private static class StorageCallBackLogger implements StorageCallback {
        @Override
        public void storageOperationResult(StorageResultCode code,
                Object message) {
            if (logger.isInfoEnabled()) {
                logger.info("Callback from storage: " + message);
            }
        }
    }

    public void invalidateStateCacheEntry(SafeKeeperId safeKeeperId) {
        serializedStateCache.remove(safeKeeperId);
    }

    public void generateCheckpoint(AbstractPE pe) {
        InitiateCheckpointingEvent initiateCheckpointingEvent = new InitiateCheckpointingEvent(
                pe.getSafeKeeperId());

        List<List<String>> compoundKeyNames;
        if (pe.getKeyValueString() == null) {
            logger.warn("Only keyed PEs can be checkpointed. Current PE ["
                    + pe.getSafeKeeperId() + "] will not be checkpointed.");
        } else {
            List<String> list = new ArrayList<String>(1);
            list.add("");
            compoundKeyNames = new ArrayList<List<String>>(1);
            compoundKeyNames.add(list);
            loopbackDispatcher.dispatchEvent(pe.getId() + "_checkpointing",
                    compoundKeyNames, initiateCheckpointingEvent);
        }
    }

    public void initiateRecovery(SafeKeeperId safeKeeperId) {
        RecoveryEvent recoveryEvent = new RecoveryEvent(safeKeeperId);
        loopbackDispatcher.dispatchEvent(safeKeeperId.getPrototypeId()
                + "_recovery",
                recoveryEvent);
    }

    public void setStateStorage(StateStorage stateStorage) {
        this.stateStorage = stateStorage;
    }

    public StateStorage getStateStorage() {
        return stateStorage;
    }

    public void setLoopbackDispatcher(Dispatcher dispatcher) {
        this.loopbackDispatcher = dispatcher;
        signalReady.countDown();
    }

    public Dispatcher getLoopbackDispatcher() {
        return this.loopbackDispatcher;
    }

    public void cacheSerializedState(SafeKeeperId safeKeeperId, byte[] state) {
        serializedStateCache.putIfAbsent(safeKeeperId, state);
    }

    public boolean isCached(SafeKeeperId safeKeeperId) {
        return serializedStateCache.containsKey(safeKeeperId);
    }

    public Set<SafeKeeperId> getKeysToRecover() {
        return keysToRecover;
    }

    public void setSerializer(SerializerDeserializer serializer) {
        this.serializer = serializer;

    }

    public SerializerDeserializer getSerializer() {
        return serializer;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
        signalReady.countDown();
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setHasher(Hasher hasher) {
        this.hasher = hasher;
        signalReady.countDown();
    }

    public Hasher getHasher() {
        return hasher;
    }

    public CountDownLatch getReadySignal() {
        return signalReady;
    }

}
