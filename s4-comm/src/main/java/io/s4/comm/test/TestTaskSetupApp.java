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

import io.s4.comm.util.CommUtil;
import io.s4.comm.util.JSONUtil;
import io.s4.comm.util.ConfigParser.Cluster.ClusterType;
import io.s4.comm.zk.ZkTaskSetup;
import io.s4.comm.zk.ZkTaskManager;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class TestTaskSetupApp {

    public static void main(String[] args) throws Exception {
        new TestTaskSetupApp().testTaskSetup1();
    }

    // test the case
    public void testTaskSetup1() throws Exception {
        String address = "effortfell.greatamerica.corp.yahoo.com:2181";
        Watcher watcher = new Watcher() {

            @Override
            public void process(WatchedEvent event) {

            }

        };
        // setup
        ZooKeeper zk = new ZooKeeper(address, 30000, watcher);
        String root = "/tasksetup_app_test";
        ZkTaskSetup zkSetup = new ZkTaskSetup(address, root, ClusterType.S4);
        Map<String, String> task1 = new HashMap<String, String>();
        task1.put("name", "task-1");

        Map<String, String> task2 = new HashMap<String, String>();
        task2.put("name", "task-2");
        String tasksListRoot = root + "/tasks";
        zkSetup.cleanUp();
        Stat exists = zk.exists(tasksListRoot, false);
        myassert(exists == null);
        Object[] data = new Object[] { task1, task2 };
        zkSetup.setUpTasks(data);

        // verify that tasks are created
        exists = zk.exists(tasksListRoot, false);
        myassert(exists != null);
        List<String> children = zk.getChildren(tasksListRoot, false);
        myassert(children.size() == data.length);
        boolean[] matched = new boolean[data.length];
        for (String child : children) {
            System.out.println(child);
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
            myassert(matched[i]);
        }

        // try running again and make verify new node is not created
        Stat oldStat = zk.exists(tasksListRoot, false);
        System.out.println("oldStat=" + oldStat);
        zkSetup.setUpTasks(data);
        Stat newStat = zk.exists(tasksListRoot, false);
        System.out.println("newstat=" + newStat);
        myassert(oldStat.getMtime() == newStat.getMtime());

        // make change to task config and try running again and verify new
        // config is uploaded
        oldStat = zk.exists(tasksListRoot, false);
        System.out.println("oldStat=" + oldStat.getVersion());
        ((Map<String, String>) data[data.length - 1]).put("name", "changedname");
        zkSetup.setUpTasks(data);
        newStat = zk.exists(tasksListRoot, false);
        System.out.println("newstat=" + newStat.getVersion());
        System.out.println();
        myassert(oldStat.getMtime() != newStat.getMtime());

        // ensure version change is working
        zkSetup.setUpTasks("1.0.0.0", data);
    }

    private void myassert(boolean b) {
        if (!b) {
            throw new AssertionError();
        }
    }
}
