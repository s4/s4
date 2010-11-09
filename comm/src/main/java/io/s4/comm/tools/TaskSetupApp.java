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

import io.s4.comm.util.ConfigUtils;
import io.s4.comm.util.ConfigParser;
import io.s4.comm.util.ConfigParser.Cluster;
import io.s4.comm.util.ConfigParser.Config;
import io.s4.comm.zk.ZkTaskSetup;

import java.io.File;
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
            printusage("Must specify at least one of of clean, setup.");
        }
        doMain(zkAddress, clean, setup, setupXml);
    }

    private static void printusage(String message) {
        System.err.println(message);
        System.err.println("java TaskSetupApp <zk_address> [clean|setup] setup_config_xml");
        System.exit(1);
    }

    private static void doMain(String zkAddress, boolean clean, boolean setup, String setupXml) {
        ConfigParser parser = new ConfigParser();
        Config config = parser.parse(setupXml);
        for (Cluster cluster : config.getClusters()) {
            processCluster(clean, zkAddress, cluster, config.getVersion());
        }
    }

    private static void processCluster(boolean clean, String zkAddress, Cluster cluster, String version) {
        List<Map<String,String>> clusterInfo = ConfigUtils.readConfig(cluster, cluster.getName(), cluster.getType(), false);
        ZkTaskSetup zkSetup = new ZkTaskSetup(zkAddress, cluster.getName(), cluster.getType());
        if (clean) {
            zkSetup.cleanUp();
        }
        
        zkSetup.setUpTasks(version, clusterInfo.toArray());
    }
}
