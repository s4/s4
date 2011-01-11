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

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ListenerProcess {
    static Logger logger = Logger.getLogger(ListenerProcess.class);
    private final String zkaddress;
    private final String clusterName;
    private String listenerRoot;
    private GenericListener genericListener;
    private Deserializer deserializer;
    private CommEventCallback callbackHandler;

    public ListenerProcess(String zkaddress, String clusterName) {
        this.zkaddress = zkaddress;
        this.clusterName = clusterName;
    }

    /**
     * This will be a blocking call and will wait until it gets a task
     * 
     * @return
     */
    public Object acquireTaskAndCreateListener(Map<String, String> map) {
        TaskManager manager = CommServiceFactory.getTaskManager(zkaddress,
                                                                clusterName,
                                                                ClusterType.S4,
                                                                callbackHandler);
        logger.info("Waiting for task");
        Object listenerConfig = manager.acquireTask(map);
        createListenerFromConfig(listenerConfig);
        return listenerConfig;
    }

    public void createListenerFromConfig(Object listenerConfig) {
        logger.info("Starting listener with config: " + listenerConfig);
        if (deserializer != null) {
            genericListener = new GenericListener(zkaddress,
                                                  clusterName,
                                                  listenerConfig,
                                                  deserializer);
        } else {
            genericListener = new GenericListener(zkaddress,
                                                  clusterName,
                                                  listenerConfig);
        }
        genericListener.start();

    }

    public Deserializer getDeserializer() {
        return deserializer;
    }

    public void setDeserializer(Deserializer deserializer) {
        this.deserializer = deserializer;
    }

    public Object listen() {
        return genericListener.receive();
    }

    public CommEventCallback getCallbackHandler() {
        return callbackHandler;
    }

    public void setCallbackHandler(CommEventCallback callbackHandler) {
        this.callbackHandler = callbackHandler;
    }

}
