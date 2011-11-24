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
package org.apache.s4.comm.zk;

import org.apache.s4.comm.core.CommEventCallback;
import org.apache.s4.comm.core.DefaultWatcher;
import org.apache.s4.comm.core.TaskManager;
import org.apache.s4.comm.util.JSONUtil;
import org.apache.s4.comm.util.ConfigParser.Cluster.ClusterType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZkTaskManager extends DefaultWatcher implements TaskManager {
    static Logger logger = Logger.getLogger(ZkTaskManager.class);
    String tasksListRoot;
    String processListRoot;

    public ZkTaskManager(String address, String ClusterName, ClusterType clusterType) {
        this(address, ClusterName, clusterType, null);
    }

    /**
     * Constructor of TaskManager
     * 
     * @param address
     * @param ClusterName
     */
    public ZkTaskManager(String address, String ClusterName, ClusterType clusterType,
            CommEventCallback callbackHandler) {
        super(address, callbackHandler);
        this.root = "/" + ClusterName + "/" + clusterType.toString();
        this.tasksListRoot = root + "/task";
        this.processListRoot = root + "/process";
    }

    /**
     * This will block the process thread from starting the task, when it is
     * unblocked it will return the data stored in the task node. This data can
     * be used by the This call assumes that the tasks are already set up
     * 
     * @return Object containing data related to the task
     */
    @Override
    public Object acquireTask(Map<String, String> customTaskData) {
        while (true) {
            synchronized (mutex) {
                try {
                    Stat tExists = zk.exists(tasksListRoot, false);
                    if (tExists == null) {
                        logger.error("Tasks znode:" + tasksListRoot
                                + " not setup.Going to wait");
                        tExists = zk.exists(tasksListRoot, true);
                        if (tExists == null) {
                            mutex.wait();
                        }
                        continue;
                    }
                    Stat pExists = zk.exists(processListRoot, false);
                    if (pExists == null) {
                        logger.error("Process root znode:" + processListRoot
                                + " not setup.Going to wait");
                        pExists = zk.exists(processListRoot, true);
                        if (pExists == null) {
                            mutex.wait();
                        }
                        continue;
                    }
                    // setting watch true to tasks node will trigger call back
                    // if there is any change to task node,
                    // this is useful to add additional tasks
                    List<String> tasks = zk.getChildren(tasksListRoot, true);
                    List<String> processes = zk.getChildren(processListRoot,
                                                            true);
                    if (processes.size() < tasks.size()) {
                        ArrayList<String> tasksAvailable = new ArrayList<String>();
                        for (int i = 0; i < tasks.size(); i++) {
                            tasksAvailable.add("" + i);
                        }
                        if (processes != null) {
                            for (String s : processes) {
                                String taskId = s.split("-")[1];
                                tasksAvailable.remove(taskId);
                            }
                        }
                        // try pick up a random task
                        Random random = new Random();
                        int id = Integer.parseInt(tasksAvailable.get(random.nextInt(tasksAvailable.size())));
                        String pNode = processListRoot + "/" + "task-" + id;
                        String tNode = tasksListRoot + "/" + "task-" + id;
                        Stat pNodeStat = zk.exists(pNode, false);
                        if (pNodeStat == null) {
                            Stat tNodeStat = zk.exists(tNode, false);
                            byte[] bytes = zk.getData(tNode, false, tNodeStat);
                            Map<String, Object> map = (Map<String, Object>) JSONUtil.getMapFromJson(new String(bytes));
                            // if(!map.containsKey("address")){
                            // map.put("address",
                            // InetAddress.getLocalHost().getHostName());
                            // }
                            if (customTaskData != null) {
                                for (String key : customTaskData.keySet()) {
                                    if (!map.containsKey(key)) {
                                        map.put(key, customTaskData.get(key));
                                    }
                                }

                            }
                            map.put("taskSize", "" + tasks.size());
                            map.put("tasksRootNode", tasksListRoot);
                            map.put("processRootNode", processListRoot);
                            String create = zk.create(pNode,
                                                      JSONUtil.toJsonString(map)
                                                              .getBytes(),
                                                      Ids.OPEN_ACL_UNSAFE,
                                                      CreateMode.EPHEMERAL);
                            logger.info("Created process Node:" + pNode + " :"
                                    + create);
                            return map;
                        }
                    } else {
                        // all the tasks are taken up, will wait for the
                        logger.info("No task available to take up. Going to wait");
                        mutex.wait();
                    }
                } catch (KeeperException e) {
                    logger.info("Warn:mostly ignorable " + e.getMessage(), e);
                } catch (InterruptedException e) {
                    logger.info("Warn:mostly ignorable " + e.getMessage(), e);
                }
            }
        }
    }
}
