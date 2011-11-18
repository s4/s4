/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package org.apache.s4.ft;

import org.apache.s4.ft.SafeKeeper.StorageResultCode;

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
