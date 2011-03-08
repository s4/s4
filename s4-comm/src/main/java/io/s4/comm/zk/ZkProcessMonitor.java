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
package io.s4.comm.zk;

import io.s4.comm.core.CommEventCallback;
import io.s4.comm.core.DefaultWatcher;
import io.s4.comm.core.ProcessMonitor;
import io.s4.comm.util.JSONUtil;
import io.s4.comm.util.ConfigParser.Cluster.ClusterType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class ZkProcessMonitor extends DefaultWatcher implements Runnable,
        ProcessMonitor {
    static Logger logger = Logger.getLogger(ZkProcessMonitor.class);
    private List<Object> destinationList;
    private Map<Integer, Object> destinationMap;
    private String processZNode;
    private Object lock = new Object();
    private volatile boolean updateMode = false;
    private String taskZNode;
    private int taskCount;

    public ZkProcessMonitor(String address, String clusterName, ClusterType clusterType) {
        this(address, clusterName, clusterType, null);
    }

    public ZkProcessMonitor(String address, String ClusterName, ClusterType clusterType,
            CommEventCallback callbackHandler) {
        super(address, callbackHandler);
        String root = "/" + ClusterName + "/" + clusterType.toString();
        this.taskZNode = root + "/task";
        this.processZNode = root + "/process";
        destinationMap = new HashMap<Integer, Object>();
        destinationList = new ArrayList<Object>();
    }

    public void monitor() {
        synchronized (mutex) {
            readConfig();
        }
        new Thread(this).start();
    }

    private void readConfig() {
        try {
            synchronized (lock) {
                Map<Integer, Object> tempDestinationMap = new HashMap<Integer, Object>();
                List<Object> tempDestinationList = new ArrayList<Object>();
                updateMode = true;
                List<String> tasks = zk.getChildren(taskZNode, false);
                this.taskCount = tasks.size();
                List<String> children = zk.getChildren(processZNode, false);
                for (String name : children) {
                    Stat stat = zk.exists(processZNode + "/" + name, false);
                    if (stat != null) {
                        byte[] data = zk.getData(processZNode + "/" + name,
                                                 false,
                                                 stat);
                        Map<String, Object> map = (Map<String, Object>) JSONUtil.getMapFromJson(new String(data));
                        String key = (String) map.get("partition");
                        if (key != null) {
                            tempDestinationMap.put(Integer.parseInt(key), map);
                        }
                        tempDestinationList.add(map);
                    }
                }
                destinationList.clear();
                destinationMap.clear();
                destinationList.addAll(tempDestinationList);
                destinationMap.putAll(tempDestinationMap);
                logger.info("Updated Destination List to" + destinationList);
                logger.info("Updated Destination Map to" + destinationMap);
            }
        } catch (KeeperException e) {
            logger.warn("Ignorable exception if it happens once in a while", e);
        } catch (InterruptedException e) {
            logger.error("Interrupted exception cause while reading process znode",
                         e);
        } finally {
            updateMode = false;
        }
    }

    public void run() {
        try {
            while (true) {
                synchronized (mutex) {
                    // set watch
                    logger.info("Setting watch on " + processZNode);
                    zk.getChildren(processZNode, true);
                    readConfig();
                    mutex.wait();
                }
            }
        } catch (KeeperException e) {
            logger.warn("KeeperException in ProcessMonitor.run", e);
        } catch (InterruptedException e) {
            logger.warn("InterruptedException in ProcessMonitor.run", e);
        }
    }

    public List<Object> getDestinationList() {
        if (updateMode) {
            synchronized (lock) {
                return destinationList;
            }
        } else {
            return destinationList;
        }
    }

    public Map<Integer, Object> getDestinationMap() {
        if (updateMode) {
            synchronized (lock) {
                return destinationMap;
            }
        } else {
            return destinationMap;
        }
    }

    @Override
    public int getTaskCount() {
        return taskCount;
    }
}
