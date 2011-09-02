package io.s4.ft;


/**
 * 
 * Encapsulates a checkpoint request. It is scheduled by the checkpointing framework.
 *
 */
public class SaveStateTask implements Runnable {
    
    SafeKeeperId safeKeeperId;
    byte[] state;
    StorageCallback storageCallback;
    StateStorage stateStorage;
    
    public SaveStateTask(SafeKeeperId safeKeeperId, byte[] state, StorageCallback storageCallback, StateStorage stateStorage) {
        super();
        this.safeKeeperId = safeKeeperId;
        this.state = state;
        this.storageCallback = storageCallback;
        this.stateStorage = stateStorage;
    }
    
    @Override
    public void run() {
        stateStorage.saveState(safeKeeperId, state, storageCallback);
    }
}