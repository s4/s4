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
import io.s4.comm.core.ProcessMonitor;
import io.s4.comm.util.Config;
import io.s4.comm.util.ConfigParser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class StaticProcessMonitor implements ProcessMonitor {
    static Logger logger = Logger.getLogger(StaticProcessMonitor.class);
    private List<Object> destinationList;
    private Map<Integer, Object> destinationMap;
    private int taskCount;
    private final String root;

    public StaticProcessMonitor(String address, String root) {
        this.root = root;
        destinationMap = new HashMap<Integer, Object>();
        destinationList = new ArrayList<Object>();

    }

    public void monitor() {
        readConfig();
    }

    private void readConfig() {
        String configfile = root + ".xml";
        Config config = ConfigParser.parse(configfile);
        List<String> processList = config.getList("process.list");
        // REDUNDANT PROCESS LIST, current design supports only one PROCESS per
        // xml
        for (String process : processList) {
            Map<String, String> paramsMap = config.getMap(process + ".config");
            loadProcess(paramsMap);
        }

    }

    private void loadProcess(Map<String, String> paramsMap) {
        if (logger.isDebugEnabled()) {
            logger.debug("Loading process params:" + paramsMap);
        }
        int numProcess = Integer.parseInt(paramsMap.get("num.process"));
        Object[] data = new Object[numProcess];
        for (int i = 0; i < numProcess; i++) {
            data[i] = new HashMap<String, String>();
        }
        for (String key : paramsMap.keySet()) {
            String val = paramsMap.get(key);
            String[] split = val.split(",");
            for (int i = 0; i < numProcess; i++) {
                if (split.length == 1) {
                    ((Map<String, String>) data[i]).put(key, split[0]);
                } else if (split.length == numProcess) {
                    ((Map<String, String>) data[i]).put(key, split[i]);
                } else {
                    // TODO:support sequential
                    throw new RuntimeException("Invalid entry in configuration: Must match either 1 or num.process: "
                            + val);
                }
            }
        }
        for (int i = 0; i < numProcess; i++) {
            Map<String, String> map = (Map<String, String>) data[i];
            String key = (String) map.get("partition");
            if (key != null) {
                destinationMap.put(Integer.parseInt(key), map);
            }
            destinationList.add(map);
        }
        this.taskCount = numProcess;
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
