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
package io.s4.comm.file;

import io.s4.comm.core.CommEventCallback;
import io.s4.comm.core.CommLayerState;
import io.s4.comm.core.TaskManager;
import io.s4.comm.util.ConfigUtils;
import io.s4.comm.util.SystemUtils;
import io.s4.comm.util.ConfigParser.Cluster.ClusterType;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class StaticTaskManager implements TaskManager {
    static Logger logger = Logger.getLogger(StaticTaskManager.class);
    Set<Map<String, String>> processSet = new HashSet<Map<String, String>>();
    private final String clusterName;
    private final ClusterType clusterType;

    /**
     * Constructor of TaskManager
     * 
     * @param address
     * @param name
     */
    public StaticTaskManager(String address, String clusterName,
            ClusterType clusterType, CommEventCallback callbackHandler) {
        this.clusterName = clusterName;
        this.clusterType = clusterType;
        // read the configuration file
        readStaticConfig();
        if (callbackHandler != null) {
            Map<String, Object> eventData = new HashMap<String, Object>();
            eventData.put("state", CommLayerState.INITIALIZED);
            callbackHandler.handleCallback(eventData);
        }
    }

    private void readStaticConfig() {
        // It should be available in classpath
        List<Map<String, String>> processList = ConfigUtils.readConfig("clusters.xml",
                                                                             clusterName,
                                                                             clusterType,
                                                                             true);

        processSet.addAll(processList);
    }

    /**
     * Will clean up taskList Node and process List Node
     */
    public boolean cleanUp() {
        throw new UnsupportedOperationException("cleanUp Not supported in red button mode");
    }

    /**
     * Creates task nodes.
     * 
     * @param numTasks
     * @param data
     */
    public void setUpTasks(int numTasks, Object[] data) {
        throw new UnsupportedOperationException("setUpTasks Not supported in red button mode");
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
            try {
                for (Map<String, String> processConfig : processSet) {
                    boolean processAvailable = canTakeupProcess(processConfig);
                    logger.info("processAvailable:" + processAvailable);
                    if (processAvailable) {
                        boolean success = takeProcess(processConfig);
                        logger.info("Acquire task:"
                                + ((success) ? "Success" : "failure"));
                        if (success) {
                            return processConfig;
                        }
                    }
                }
                Thread.sleep(5000);
            } catch (Exception e) {
                logger.error("Exception in acquireTask Method:"
                        + customTaskData, e);
            }
        }
    }

    private boolean takeProcess(Map<String, String> processConfig) {
        File lockFile = null;
        try {
            // TODO:contruct from processConfig
            String lockFileName = createLockFileName(processConfig);
            lockFile = new File(lockFileName);
            if (!lockFile.exists()) {
                FileOutputStream fos = new FileOutputStream(lockFile);
                FileLock fl = fos.getChannel().tryLock();
                if (fl != null) {
                    String message = "Task acquired by PID:"
                            + SystemUtils.getPID() + " HOST:"
                            + InetAddress.getLocalHost().getHostName();
                    fos.write(message.getBytes());
                    fos.close();
                    logger.info(message + "  Lock File location: "
                            + lockFile.getAbsolutePath());
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("Exception trying to take up process:" + processConfig,
                         e);
        } finally {
            if (lockFile != null) {
                lockFile.deleteOnExit();
            }
        }
        return false;
    }

    private String createLockFileName(Map<String, String> processConfig) {
        String lockDir = System.getProperty("lock_dir");
        String lockFileName = clusterName + processConfig.get("ID");
        if (lockDir != null && lockDir.trim().length() > 0) {
            File file = new File(lockDir);
            if (!file.exists()) {
                file.mkdirs();
            }
            return lockDir + "/" + lockFileName;
        } else {
            return lockFileName;
        }
    }

    private boolean canTakeupProcess(Map<String, String> processConfig) {
        String host = processConfig.get("process.host");
        try {
            InetAddress inetAddress = InetAddress.getByName(host);
            logger.info("Host Name: "
                    + InetAddress.getLocalHost().getCanonicalHostName());
            if (!host.equals("localhost")) {
                if (!InetAddress.getLocalHost().equals(inetAddress)) {
                    return false;
                }
            }
        } catch (Exception e) {
            logger.error("Invalid host:" + host);
            return false;
        }
        String lockFileName = createLockFileName(processConfig);
        File lockFile = new File(lockFileName);
        if (!lockFile.exists()) {
            return true;
        } else {
            logger.info("Process taken up by another process lockFile:"
                    + lockFileName);
        }
        return false;
    }

}
