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
package io.s4.comm.tools;

import io.s4.comm.util.Config;
import io.s4.comm.util.ConfigParser;
import io.s4.comm.zk.ZkTaskManager;
import io.s4.comm.zk.ZkTaskSetup;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class will set up initial tasks on the zookeeper USAGE: java AppTask
 * [clean] setup config.xml
 * 
 * @author kishoreg
 * 
 */
public class TaskSetupApp {
    public static void main(String[] args) {
        String zkAddress = "";
        boolean clean = false;
        boolean setup = false;
        String setupXml = null;
        for (int i = 0; i < args.length; i++) {
            if (i == 0) {
                zkAddress = args[0];
            }
            if (args[i].equals("clean")) {
                clean = true;
            } else if (args[i].equals("setup")) {
                setup = true;
            } else if (i == args.length - 1) {
                setupXml = args[i];
            }
        }
        if (setupXml == null || !new File(setupXml).exists()) {
            printusage("Set up xml: " + setupXml + " does not exist");
        }
        if (!setup && !clean) {
            System.err.println("Invalid usage.");
            printusage("Must specify atleast one of of clean, setup.");
        }
        doMain(zkAddress, clean, setup, setupXml);
    }

    private static void printusage(String message) {
        System.err.println(message);
        System.err.println("java TaskSetupApp <zk_address> [clean|setup] setup_config_xml");
        System.exit(1);
    }

    private static void doMain(String zkAddress, boolean clean, boolean setup, String setupXml) {
        Config config = ConfigParser.parse(setupXml);
        List<String> tasksList = config.getList("tasks.list");
        for (String task : tasksList) {
            String version = config.getString(task + ".version");
            Map<String, String> paramsMap = config.getMap(task + ".config");
            processTask(clean, zkAddress, paramsMap, version);
        }
    }

    private static void processTask(boolean clean, String zkAddress, Map<String, String> paramsMap, String version) {
        System.out.println(paramsMap);
        String appName = paramsMap.get("app.name");
        int numTasks = Integer.parseInt(paramsMap.get("num.tasks"));
        String taskType = paramsMap.get("task.type");
        String root = "/" + appName + "/" + taskType;
        System.out.println("root:" + root);
        ZkTaskSetup zkSetup = new ZkTaskSetup(zkAddress, root);
        if (clean) {
            zkSetup.cleanUp();
        }
        Object[] data = new Object[numTasks];

        for (int i = 0; i < numTasks; i++) {
            data[i] = new HashMap<String, String>();
        }

        for (String key : paramsMap.keySet()) {
            String val = paramsMap.get(key);
            String[] split = val.split(",");
            for (int i = 0; i < numTasks; i++) {
                if (split.length == 1) {
                    ((Map<String, String>) data[i]).put(key, split[0]);
                } else if (split.length == numTasks) {
                    ((Map<String, String>) data[i]).put(key, split[i]);
                } else {
                    // TODO:support sequential
                    throw new RuntimeException("Invalid entry in configuration: Must match either 1 or num.tasks: "
                            + val);
                }
            }
        }
        zkSetup.setUpTasks(version, numTasks, data);
        /*
         * if (taskType.equals("listener")) { Object[] data = new
         * Object[numTasks]; int startPort =
         * Integer.parseInt(paramsMap.get("port.start")); boolean
         * enablePartition = Boolean.valueOf(paramsMap.get("enable.partition"));
         * for (int i = 0; i < numTasks; i++) { Map<String, String> map = new
         * HashMap<String, String>(); if (mode.equals("multicast")) {
         * map.put("address", paramsMap.get("address")); map.put("port", "" +
         * (startPort + i)); } else if (mode.equals("unicast")) {
         * map.put("port", "" + (startPort + i)); } map.put("mode", mode);
         * if(enablePartition){ map.put("partition", "" + i); } data[i] = map; }
         * manager.setUpTasks(numTasks, data); }else if
         * (taskType.equals("sender")) { Object[] data = new Object[numTasks];
         * for (int i = 0; i < numTasks; i++) { Map<String, String> map = new
         * HashMap<String, String>(); map.put("mode", mode); data[i] = map; }
         * manager.setUpTasks(numTasks, data); }else{ throw new
         * RuntimeException("UNKNOWN TASK TYPE"); }
         */
    }
}
