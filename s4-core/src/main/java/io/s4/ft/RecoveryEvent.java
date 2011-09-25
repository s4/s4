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
package io.s4.ft;

/**
 * 
 * Event that triggers the recovery of a checkpoint. 
 *
 */
public class RecoveryEvent extends CheckpointingEvent {

    public RecoveryEvent() {
        // as required by default kryo serializer
    }

    public RecoveryEvent(SafeKeeperId safeKeeperId) {
        super(safeKeeperId);
    }

}
