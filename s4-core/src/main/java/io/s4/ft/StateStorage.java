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
    public void saveState(SafeKeeperId key, byte[] state, StorageCallback callback);

    /**
     * <p>
     * <i>Synchronous</i> call to fetch stored checkpoint data.
     * </p>
     * <p>
     * Must return null if storage does not contain this key.
     * </p>
     * 
     * @param key
     *            safeKeeperId for this checkpoint
     * 
     * @return stored checkpoint data, or null if the storage does not contain
     *         data for the given key
     */
    public byte[] fetchState(SafeKeeperId key);

    /**
     * <i>Synchronous</i> call to fetch all stored safeKeeper Ids.
     * 
     * @return all stored safeKeeper Ids.
     */
    public Set<SafeKeeperId> fetchStoredKeys();

}
