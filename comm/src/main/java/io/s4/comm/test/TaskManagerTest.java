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
package io.s4.comm.test;

import io.s4.comm.core.TaskManager;
import io.s4.comm.file.StaticTaskManager;
import io.s4.comm.util.ConfigParser.Cluster.ClusterType;
import io.s4.comm.zk.ZkTaskSetup;
import io.s4.comm.zk.ZkTaskManager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class TaskManagerTest {
    public static void main(String[] args) throws Exception {
        // testZkTaskManager(args);
        testStaticTaskManager(args);
        Thread.sleep(10000);
    }

    private static void testStaticTaskManager(String[] args) {
        String address = null;
        address = "localhost:2181";
        TaskManager taskManager = new StaticTaskManager(address,
                                                        "taskmanagerTest",
                                                        ClusterType.S4,
                                                        null);
        Map<String, String> customTaskData = new HashMap<String, String>();
        Object acquireTask = taskManager.acquireTask(customTaskData);
        System.out.println("Acuired Task:" + acquireTask);

    }

    private static void testZkTaskManager(String[] args) {
        System.out.println("Here");
        // "effortfell.greatamerica.corp.yahoo.com:2181"
        String address = args[0];
        address = "localhost:2181";
        String processName = args[1];
        ZkTaskSetup taskSetup = new ZkTaskSetup(address,
                                                      "/taskmanagerTest",
                                                      ClusterType.S4);
        taskSetup.cleanUp();
        taskSetup.setUpTasks("1.0.0.0", new String[] { "task0", "task1" });
        Object obj;
        System.out.println(processName + " Going to Wait for a task");
        HashMap<String, String> map = new HashMap<String, String>();
        ZkTaskManager taskManager = new ZkTaskManager(address,
                                                      "/taskmanagerTest",
                                                      ClusterType.S4);
        obj = taskManager.acquireTask(map);
        System.out.println(processName + "taking up task: " + obj);
        File f = new File("c:/" + obj + ".file");
        f.delete();
        while (true) {
            if (f.exists()) {
                break;
            }
            System.out.println(processName + " processing task: " + obj);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Exiting task:" + obj);
    }
}
