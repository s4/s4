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
import io.s4.comm.util.CommUtil;
import io.s4.comm.util.JSONUtil;
import io.s4.comm.util.ConfigParser.Cluster.ClusterType;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZkTaskSetup extends DefaultWatcher {
    static Logger logger = Logger.getLogger(ZkTaskSetup.class);
    String tasksListRoot;
    String processListRoot;

    public ZkTaskSetup(String address, String clusterName, ClusterType clusterType) {
        this(address, clusterName, clusterType, null);
    }

    /**
     * Constructor of ZkTaskSetup
     * 
     * @param address
     * @param name
     */
    public ZkTaskSetup(String address, String clusterName, ClusterType clusterType,
            CommEventCallback callbackHandler) {
        super(address, callbackHandler);
        
        this.root = "/" + clusterName + "/" + clusterType.toString();
        this.tasksListRoot = root + "/task";
        this.processListRoot = root + "/process";
    }

    public void setUpTasks(Object[] data) {
        setUpTasks("-1", data);
    }

    /**
     * Creates task nodes.
     * 
     * @param numTasks
     * @param data
     */
    public void setUpTasks(String version, Object[] data) {
        try {
            logger.info("Trying to set up configuration with new version:"
                    + version);
            if (!version.equals("-1")) {
                if (!isConfigVersionNewer(version)) {
                    logger.info("Config version not newer than current version");
                    return;
                } else {
                    cleanUp();
                }
            } else {
                logger.info("Not checking version number since it is set to -1");
            }

            // check if config data newer
            if (!isConfigDataNewer(data)) {
                logger.info("Config data not newer than current version");
                return;
            } else {
                logger.info("Found newer Config data. Cleaning old data");
                cleanUp();
            }

            // Create ZK node name
            if (zk != null) {
                Stat s;
                s = zk.exists(root, false);
                if (s == null) {
                    String parent = new File(root).getParent()
                                                  .replace(File.separatorChar,
                                                           '/');
                    if (logger.isDebugEnabled()) {
                        logger.debug("parent:" + parent);
                    }
                    Stat exists = zk.exists(parent, false);
                    if (exists == null) {
                        zk.create(parent,
                                  new byte[0],
                                  Ids.OPEN_ACL_UNSAFE,
                                  CreateMode.PERSISTENT);
                    }
                    zk.create(root,
                              new byte[0],
                              Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
                }
            }
            Stat s;
            s = zk.exists(tasksListRoot, false);
            if (s == null) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("config.version", version);
                String jsonString = JSONUtil.toJsonString(map);
                zk.create(tasksListRoot,
                          jsonString.getBytes(),
                          Ids.OPEN_ACL_UNSAFE,
                          CreateMode.PERSISTENT);
            }
            s = zk.exists(processListRoot, false);
            if (s == null) {
                zk.create(processListRoot,
                          new byte[0],
                          Ids.OPEN_ACL_UNSAFE,
                          CreateMode.PERSISTENT);

            }

            for (int i = 0; i < data.length; i++) {
                String nodeName = tasksListRoot + "/" + "task" + "-" + i;
                Stat sTask = zk.exists(nodeName, false);
                if (sTask == null) {
                    logger.info("Creating taskNode: " + nodeName);
                    byte[] byteBuffer = JSONUtil.toJsonString(data[i])
                                                .getBytes();
                    zk.create(nodeName,
                              byteBuffer,
                              Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
                } else {
                    logger.warn("TaskNode already exisits: " + nodeName);
                }
            }
        } catch (Exception e) {
            logger.error("Keeper exception when creating task nodes: "
                                 + e.toString(),
                         e);
            throw new RuntimeException(e);
        }
    }

    private boolean isConfigDataNewer(Object[] data) {
        try {
            Stat s;
            s = zk.exists(tasksListRoot, false);
            if (s != null) {
                List<String> children = zk.getChildren(tasksListRoot, false);
                if (children.size() != data.length) {
                    return true;
                }
                boolean[] matched = new boolean[data.length];
                for (String child : children) {
                    String childPath = tasksListRoot + "/" + child;
                    Stat sTemp = zk.exists(childPath, false);
                    byte[] tempData = zk.getData(tasksListRoot + "/" + child,
                                                 false,
                                                 sTemp);
                    Map<String, Object> map = (Map<String, Object>) JSONUtil.getMapFromJson(new String(tempData));

                    // check if it matches any of the data
                    for (int i = 0; i < data.length; i++) {
                        Map<String, Object> newData = (Map<String, Object>) data[i];
                        if (!matched[i] && CommUtil.compareMaps(newData, map)) {
                            matched[i] = true;
                            break;
                        }
                    }
                }
                for (int i = 0; i < matched.length; i++) {
                    if (!matched[i]) {
                        return true;
                    }
                }
            } else {
                return true;
            }
        } catch (Exception e) {
            throw new RuntimeException(" Exception in isConfigDataNewer method ",
                                       e);
        }
        return false;
    }

    private boolean isConfigVersionNewer(String version) throws Exception {
        Stat s;
        s = zk.exists(tasksListRoot, false);
        if (s != null) {
            byte[] data = zk.getData(tasksListRoot, false, s);
            if (data != null && data.length > 0) {
                String jsonString = new String(data);
                if (jsonString != null) {
                    Map<String, Object> map = JSONUtil.getMapFromJson(jsonString);
                    if (map.containsKey("config.version")) {
                        boolean update = false;
                        String currentVersion = map.get("config.version")
                                                   .toString();
                        logger.info("Current config version:" + currentVersion);
                        String[] curV = currentVersion.split("\\.");
                        String[] newV = version.split("\\.");
                        for (int i = 0; i < Math.max(curV.length, newV.length); i++) {
                            if (Integer.parseInt(newV[i]) > Integer.parseInt(curV[i])) {
                                update = true;
                                break;
                            }
                        }
                        if (!update) {
                            logger.info("Current config version is newer. Config will not be updated");
                        }
                        return update;
                    }
                } else {
                    logger.info("No data at znode " + tasksListRoot
                            + " so version checking will not be done");
                }
            } else {
                logger.info("No data at znode " + tasksListRoot
                        + " so version checking will not be done");
            }
        } else {
            logger.info("znode " + tasksListRoot
                    + " does not exist, so creating new one is fine");
        }
        return true;
    }

    /**
     * Will clean up taskList Node and process List Node
     */
    public boolean cleanUp() {
        try {
            logger.info("Cleaning :" + tasksListRoot);
            Stat exists = zk.exists(tasksListRoot, false);
            if (exists != null) {
                List<String> children = zk.getChildren(tasksListRoot, false);
                if (children.size() > 0) {
                    for (String child : children) {
                        logger.info("Cleaning :" + tasksListRoot + "/" + child);
                        zk.delete(tasksListRoot + "/" + child, 0);
                    }
                }
                zk.delete(tasksListRoot, 0);
            }

            exists = zk.exists(processListRoot, false);
            if (exists != null) {
                List<String> children = zk.getChildren(processListRoot, false);
                if (children.size() > 0) {
                    logger.warn("Some processes are already running. Cleaning them up. Might result in unpredictable behavior");
                    for (String child : children) {
                        logger.info("Cleaning :" + processListRoot + "/"
                                + child);
                        zk.delete(processListRoot + "/" + child, 0);
                    }
                }
                logger.info("Finished cleaning :" + processListRoot);
                zk.delete(processListRoot, 0);
            }
            return true;
        } catch (Exception e) {
            logger.error("Exception while cleaning up: " + e.getMessage(), e);
            return false;
        }
    }

}
