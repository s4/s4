package io.s4.ft;

import io.s4.ft.SafeKeeper.StorageResultCode;

/**
 * A factory for creating storage callbacks that simply log callback results
 * 
 * 
 */
public class LoggingStorageCallbackFactory implements StorageCallbackFactory {

    @Override
    public StorageCallback createStorageCallback() {
        return new StorageCallbackLogger();
    }

    /**
     * A basic storage callback that simply logs results from storage operations
     * 
     */
    class StorageCallbackLogger implements StorageCallback {

        @Override
        public void storageOperationResult(StorageResultCode code, Object message) {
            if (StorageResultCode.SUCCESS == code) {
                if (SafeKeeper.logger.isDebugEnabled()) {
                    SafeKeeper.logger.debug("Callback from storage: " + StorageResultCode.SUCCESS + " : " + message);
                }
            } else {
                SafeKeeper.logger.warn("Callback from storage: " +StorageResultCode.FAILURE + " : " + message);
            }
        }
    }

}
