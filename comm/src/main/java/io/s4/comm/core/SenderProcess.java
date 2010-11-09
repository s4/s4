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
package io.s4.comm.core;

import io.s4.comm.util.ConfigParser.Cluster.ClusterType;

import java.util.Map;

public class SenderProcess {
    protected final String zkaddress;
    protected final String adapterClusterName;
    protected final String s4ClusterName;
    protected Serializer serializer;
    protected CommEventCallback callbackHandler;

    public SenderProcess(String zkaddress, String clusterName) {
        this(zkaddress, clusterName, clusterName);
    }

    public SenderProcess(String zkaddress, String adapterClusterName,
            String s4ClusterName) {
        this.zkaddress = zkaddress;
        this.adapterClusterName = adapterClusterName;
        this.s4ClusterName = s4ClusterName;
    }

    public void setSerializer(Serializer serializer) {
        this.serializer = serializer;
    }

    public CommEventCallback getCallbackHandler() {
        return callbackHandler;
    }

    public void setCallbackHandler(CommEventCallback callbackHandler) {
        this.callbackHandler = callbackHandler;
    }

    protected GenericSender genericSender;

    /**
     * This will be a blocking call and will wait until it gets a task
     * 
     * @return senderConfig object, currently its map
     */

    public Object acquireTaskAndCreateSender(Map<String, String> map) {
        TaskManager manager = CommServiceFactory.getTaskManager(zkaddress,
                                                                adapterClusterName,
                                                                ClusterType.ADAPTER,
                                                                callbackHandler);
        if (callbackHandler != null) {
            // manager.setCallbackHandler(callbackHandler);
        }
        Object senderConfig = manager.acquireTask(map);
        createSenderFromConfig(senderConfig);
        return senderConfig;
    }

    public void createSenderFromConfig(Object senderConfig) {
        if (serializer != null) {
            this.genericSender = new GenericSender(zkaddress,
                                                   adapterClusterName,
                                                   s4ClusterName,
                                                   senderConfig,
                                                   serializer);
        } else {
            this.genericSender = new GenericSender(zkaddress,
                                                   adapterClusterName,
                                                   s4ClusterName,
                                                   senderConfig);
        }
        if (callbackHandler != null) {
            this.genericSender.setCallbackHandler(callbackHandler);
        }

    }

    /**
     * This method will send the data to receivers in a round robin fashion
     * 
     * @param data
     * @return
     */
    public boolean send(Object data) {
        return genericSender.send(data);
    }

    /**
     * This will send the data to a specific channel/receiver/partition
     * 
     * @param partition
     * @param data
     * @return
     */
    public boolean sendToPartition(int partition, Object data) {
        return genericSender.sendToPartition(partition, data);
    }

    /**
     * compute partition using hashcode and send to appropriate partition
     * 
     * @param partition
     * @param data
     * @return true on success, false on failure
     */

    public boolean sendUsingHashCode(int hashcode, Object data) {
        return genericSender.sendUsingHashCode(hashcode, data);
    }

    /**
     * Returns the number of partitions on the receiver app side TODO: Currently
     * it returns the number of tasks on the listener side. It works for now
     * since numofPartitions=taskCount
     */

    public int getNumOfPartitions() {
        return genericSender.getListenerTaskCount();
    }

}
