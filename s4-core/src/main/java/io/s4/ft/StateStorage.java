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
     * Stores a checkpoint.
     * 
     * <p>
     * NOTE: we don't handle any failure/success return value, because all
     * failure/success notifications go through the StorageCallback reference
     * </p>
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
     * Fetches data for a stored checkpoint.
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
     * Fetches all stored safeKeeper Ids.
     * 
     * @return all stored safeKeeper Ids.
     */
    public Set<SafeKeeperId> fetchStoredKeys();

}
