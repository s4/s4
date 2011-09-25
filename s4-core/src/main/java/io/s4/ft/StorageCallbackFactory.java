package io.s4.ft;

/**
 * A factory for creating storage callbacks. Storage callback implementations
 * that can take specific actions upon success or failure of asynchronous
 * storage operations.
 * 
 */
public interface StorageCallbackFactory {

    /**
     * Factory method
     * 
     * @return returns a StorageCallback instance
     */
    public StorageCallback createStorageCallback();

}
