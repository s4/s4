package io.s4.ft;

import io.s4.dispatcher.Dispatcher;
import io.s4.dispatcher.partitioner.Hasher;
import io.s4.emitter.CommLayerEmitter;
import io.s4.processor.AbstractPE;
import io.s4.serialize.SerializerDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

/**
 * 
 * <p>
 * This class is responsible for coordinating interactions between the S4 event
 * processor and the checkpoint storage backend.
 * </p>
 * 
 * 
 * 
 */
public class SafeKeeper {

    public enum StorageResultCode {
        SUCCESS, FAILURE
    }

    static Logger logger = Logger.getLogger(SafeKeeper.class);
    private StateStorage stateStorage;
    private Dispatcher loopbackDispatcher;
    private SerializerDeserializer serializer;
    private Hasher hasher;
    // monitor field injection through a latch
    private CountDownLatch signalReady = new CountDownLatch(2);
    private CountDownLatch signalNodesAvailable = new CountDownLatch(1);
    private StorageCallbackFactory storageCallbackFactory = new LoggingStorageCallbackFactory();

    public SafeKeeper() {
    }

    /**
     * <p>
     * This init() method <b>must</b> be called by the dependency injection
     * framework. It waits until all required dependencies
     * are injected in SafeKeeper, and until the node count is accessible from
     * the communication layer.
     * </p>
     */
    public void init() {
        try {
            getReadySignal().await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        int nodeCount = getLoopbackDispatcher().getEventEmitter().getNodeCount();
        // required wait until nodes are available
        while (nodeCount == 0) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException ignored) {
            }
            nodeCount = getLoopbackDispatcher().getEventEmitter().getNodeCount();
        }

        signalNodesAvailable.countDown();
    }

    /**
     * Forwards a call to checkpoint a PE to the backend storage.
     * 
     * @param key
     *            safeKeeperId
     * @param state
     *            checkpoint data
     */
    public void saveState(SafeKeeperId key, byte[] state) {
        stateStorage.saveState(key, state, storageCallbackFactory.createStorageCallback());
    }

    /**
     * Fetches checkpoint data from storage for a given PE
     * 
     * @param key
     *            safeKeeperId
     * @return checkpoint data
     */
    public byte[] fetchSerializedState(SafeKeeperId key) {

        try {
            signalNodesAvailable.await();
        } catch (InterruptedException ignored) {
        }
        byte[] result = null;
        result = stateStorage.fetchState(key);
        return result;
    }

    /**
     * Generates a checkpoint event for a given PE, and enqueues it in the local
     * event queue.
     * 
     * @param pe
     *            reference to a PE
     */
    public void generateCheckpoint(AbstractPE pe) {
        InitiateCheckpointingEvent initiateCheckpointingEvent = new InitiateCheckpointingEvent(pe.getSafeKeeperId());

        List<List<String>> compoundKeyNames;
        if (pe.getKeyValueString() == null) {
            logger.warn("Only keyed PEs can be checkpointed. Current PE [" + pe.getSafeKeeperId()
                    + "] will not be checkpointed.");
        } else {
            List<String> list = new ArrayList<String>(1);
            list.add("");
            compoundKeyNames = new ArrayList<List<String>>(1);
            compoundKeyNames.add(list);
            loopbackDispatcher.dispatchEvent(pe.getId() + "_checkpointing", compoundKeyNames,
                    initiateCheckpointingEvent);
        }
    }

    /**
     * Generates a recovery event, and enqueues it in the local event queue.<br/>
     * This can be used for an eager recovery mechanism.
     * 
     * @param safeKeeperId
     *            safeKeeperId to recover
     */
    public void initiateRecovery(SafeKeeperId safeKeeperId) {
        RecoveryEvent recoveryEvent = new RecoveryEvent(safeKeeperId);
        loopbackDispatcher.dispatchEvent(safeKeeperId.getPrototypeId() + "_recovery", recoveryEvent);
    }

    public void setSerializer(SerializerDeserializer serializer) {
        this.serializer = serializer;
    }

    public SerializerDeserializer getSerializer() {
        return serializer;
    }

    public int getPartitionId() {
        return ((CommLayerEmitter) loopbackDispatcher.getEventEmitter()).getListener().getId();
    }

    public void setHasher(Hasher hasher) {
        this.hasher = hasher;
        signalReady.countDown();
    }

    public Hasher getHasher() {
        return hasher;
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

    public CountDownLatch getReadySignal() {
        return signalReady;
    }

    public StorageCallbackFactory getStorageCallbackFactory() {
        return storageCallbackFactory;
    }

    public void setStorageCallbackFactory(StorageCallbackFactory storageCallbackFactory) {
        this.storageCallbackFactory = storageCallbackFactory;
    }
    

}
