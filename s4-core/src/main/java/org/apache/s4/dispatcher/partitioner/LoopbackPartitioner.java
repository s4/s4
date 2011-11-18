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
package org.apache.s4.dispatcher.partitioner;

import org.apache.s4.emitter.CommLayerEmitter;
import org.apache.s4.emitter.EventEmitter;
import org.apache.s4.listener.EventListener;
import org.apache.s4.processor.PEContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * A partitioner that assigns events to the current partition, as given by the comm layer.
 * 
 */
public class LoopbackPartitioner implements Partitioner, VariableKeyPartitioner {

    CommLayerEmitter emitter;

    @Override
    public List<CompoundKeyInfo> partition(String streamName,
            List<List<String>> compoundKeyNames, Object event,
            int partitionCount) {
        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();
        CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
        StringBuilder compoundKeyBuilder = new StringBuilder();
        // This partitioning ignores the values of the keyed attributes;
        // it partitions to the current partition id of the pe container
        partitionInfo.setPartitionId(emitter.getListener().getId());
        for (List<String> keyNames : compoundKeyNames) {
            for (String keyName : keyNames) {
                compoundKeyBuilder.append(keyName);
            }
        }
        partitionInfo.setCompoundKey(compoundKeyBuilder.toString());
        partitionInfoList.add(partitionInfo);
        return partitionInfoList;
    }

    @Override
    public List<CompoundKeyInfo> partition(String streamName, Object event,
            int partitionCount) {
        return partition(streamName, new ArrayList<List<String>>(0), event,
                partitionCount);
    }

    /**
     * A reference on the emitter allows getting the current partition id from the comm layer 
     * @param emitter comm layer emitter
     */
    public void setEventEmitter(CommLayerEmitter emitter) {
        this.emitter = emitter;
    }
    
}
