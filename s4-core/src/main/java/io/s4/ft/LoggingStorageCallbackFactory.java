package io.s4.ft;

import io.s4.ft.SafeKeeper.StorageResultCode;

import org.apache.log4j.Logger;

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
    static class StorageCallbackLogger implements StorageCallback {

        private static Logger logger = Logger.getLogger("s4-ft");

        @Override
        public void storageOperationResult(StorageResultCode code, Object message) {
            if (StorageResultCode.SUCCESS == code) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Callback from storage: " + StorageResultCode.SUCCESS.name() + " : " + message);
                }
            } else {
                logger.warn("Callback from storage: " + StorageResultCode.FAILURE.name() + " : " + message);
            }
        }
    }

}
