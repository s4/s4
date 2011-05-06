package io.s4.ft;

/**
 * 
 * Callback for reporting the result of an asynchronous storage operation
 *
 */
public interface StorageCallback {
	
    /**
     * Notifies the result of a storage operation
     * 
     * @param resultCode code for the result : {@link SafeKeeper.StorageResultCode SafeKeeper.StorageResultCode}
     * @param message whatever message object is suitable
     */
	public void storageOperationResult(SafeKeeper.StorageResultCode resultCode,
			Object message);

}
