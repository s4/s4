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
