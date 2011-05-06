package io.s4.ft;

import java.util.Set;

/**
 * <p>
 * Defines the methods that must be implemented by backend storage for
 * checkpoints.
 * </p>
 * 
 */
public interface StateStorage {

    /**
     * <i>Asynchronous</i> call for storing the checkpoint data
     * 
     * @param key
     *            safeKeeperId
     * @param state
     *            checkpoint data as a byte array
     * @param callback
     *            callback for receiving notifications of storage operations.
     *            This callback is configurable
     */
    public void saveState(SafeKeeperId key, byte[] state,
            StorageCallback callback);

    /**
     * <i>Synchronous</i> call to fetch stored checkpoint data
     * 
     * @param key
     *            safeKeeperId for this checkpoint
     * 
     * @return stored checkpoint data
     */
    public byte[] fetchState(SafeKeeperId key);

    /**
     * <i>Synchronous</i> call to fetch all stored safeKeeper Ids.
     * 
     * @return all stored safeKeeper Ids.
     */
    public Set<SafeKeeperId> fetchStoredKeys();


}
