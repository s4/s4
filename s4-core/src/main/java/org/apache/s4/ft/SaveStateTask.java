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
