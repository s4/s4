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

import io.s4.comm.core.ProcessMonitor;
import io.s4.comm.util.ConfigUtils;
import io.s4.comm.util.ConfigParser.Cluster.ClusterType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class StaticProcessMonitor implements ProcessMonitor {
    static Logger logger = Logger.getLogger(StaticProcessMonitor.class);
    private List<Object> destinationList = new ArrayList<Object>();
    private Map<Integer, Object> destinationMap = new HashMap<Integer, Object>();
    private int taskCount;
    private final String clusterName;
    private final ClusterType clusterType;

    public StaticProcessMonitor(String address, String clusterName,
            ClusterType clusterType) {
        this.clusterName = clusterName;
        this.clusterType = clusterType;
    }

    public void monitor() {
        readConfig();
    }

    private void readConfig() {
        List<Map<String, String>> processList = ConfigUtils.readConfig("clusters.xml",
                                                                             clusterName,
                                                                             clusterType,
                                                                             true);
        for (Map<String, String> processMap : processList) {
            destinationList.add(processMap);
            String key = (String) processMap.get("partition");
            if (key != null) {
                destinationMap.put(Integer.parseInt(key), processMap);
            }
        }
        taskCount = destinationList.size();
        logger.info("Destination List: " + destinationList);
        logger.info("Destination Map: " + destinationMap);
        logger.info("TaskCount: " + taskCount);
    }

    public List<Object> getDestinationList() {
        return destinationList;
    }

    public Map<Integer, Object> getDestinationMap() {
        return destinationMap;
    }

    @Override
    public int getTaskCount() {
        return taskCount;
    }

}
