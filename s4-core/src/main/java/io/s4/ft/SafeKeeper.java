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

    private static Logger LOG = Logger.getLogger(SafeKeeper.class);
    private ConcurrentMap<SafeKeeperId, byte[]> serializedStateCache = new ConcurrentHashMap<SafeKeeperId, byte[]>(
            16, 0.75f, 2);
    private Set<SafeKeeperId> keysToRecover = Collections
            .newSetFromMap(new ConcurrentHashMap<SafeKeeperId, Boolean>(16,
                    0.75f, 2));
    private Thread eagerFetchingThread;
    private StateStorage stateStorage;
    private Dispatcher loopbackDispatcher;
    private SerializerDeserializer serializer;
    private Hasher hasher;
    // FIXME currently using partition id to identify current node
    private String partitionId;
    // monitor field injection through a latch
    private CountDownLatch signalReady = new CountDownLatch(3);

    public SafeKeeper() {
    }

    public void init() {
        
        
        // start background eager fetching thread (TODO: iff eager fetching)
        eagerFetchingThread = new Thread(new EagerSerializedStateFetcher(this),
                "EagerSerializedStateLoader");
        eagerFetchingThread.start();
    }

    public void saveState(SafeKeeperId key, byte[] state) {
        stateStorage.saveState(key, state, new StorageCallBackLogger());
    }

    public byte[] fetchSerializedState(SafeKeeperId key) {
        byte[] result = null;
        if (serializedStateCache.containsKey(key)) {
            result = serializedStateCache.remove(key);
        } else {
            result = stateStorage.fetchState(key);
        }
        keysToRecover.remove(key);
        return result;
    }

    // TODO externalize
    private static class StorageCallBackLogger implements StorageCallback {
        @Override
        public void storageOperationResult(StorageResultCode code,
                Object message) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Callback from storage: " + message);
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
            loopbackDispatcher.dispatchEvent(pe.getStreamName(),
                    initiateCheckpointingEvent);
        } else {
            List<String> list = new ArrayList<String>(1);
            list.add(pe.getKeyValueString());
            compoundKeyNames = new ArrayList<List<String>>(1);
            compoundKeyNames.add(list);
            loopbackDispatcher.dispatchEvent(pe.getStreamName(),
                    compoundKeyNames, initiateCheckpointingEvent);
        }
    }

    public void initiateRecovery(SafeKeeperId safeKeeperId) {
        RecoveryEvent recoveryEvent = new RecoveryEvent(safeKeeperId);
        loopbackDispatcher.dispatchEvent(safeKeeperId.getStreamName(),
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
