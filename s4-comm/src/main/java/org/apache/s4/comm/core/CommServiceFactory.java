/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.s4.comm.core;

import org.apache.s4.comm.file.StaticProcessMonitor;
import org.apache.s4.comm.file.StaticTaskManager;
import org.apache.s4.comm.util.ConfigParser.Cluster.ClusterType;
import org.apache.s4.comm.zk.ZkProcessMonitor;
import org.apache.s4.comm.zk.ZkTaskManager;

import org.apache.log4j.Logger;

/**
 * Common Factory class to provide appropriate implementations
 * 
 * @author kishoreg
 * 
 */
public class CommServiceFactory {
    private static Logger logger = Logger.getLogger(CommServiceFactory.class);

    public static TaskManager getTaskManager(String zkaddress,
            String clusterName, ClusterType clusterType,
            CommEventCallback callbackHandler) {
        String mode = System.getProperty("commlayer.mode");
        TaskManager taskManager = null;
        if (mode != null && mode.equalsIgnoreCase("static")) {
            logger.info("Comm layer mode is set to static");
            taskManager = new StaticTaskManager(zkaddress,
                                                clusterName,
                                                clusterType,
                                                callbackHandler);
        } else {
            taskManager = new ZkTaskManager(zkaddress,
                                            clusterName,
                                            clusterType,
                                            callbackHandler);
        }

        return taskManager;
    }

    public static ProcessMonitor getProcessMonitor(String zkaddress,
            String clusterName, CommEventCallback callbackHandler) {
        ProcessMonitor processMonitor = null;
        String mode = System.getProperty("commlayer.mode");
        if (mode != null && mode.equalsIgnoreCase("static")) {
            logger.info("Comm layer mode is set to static");
            processMonitor = new StaticProcessMonitor(zkaddress,
                                                      clusterName,
                                                      ClusterType.S4);
        } else {
            processMonitor = new ZkProcessMonitor(zkaddress,
                                                  clusterName,
                                                  ClusterType.S4,
                                                  callbackHandler);
        }
        return processMonitor;
    }

}
