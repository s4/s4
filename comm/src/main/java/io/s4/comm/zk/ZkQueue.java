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

import io.s4.comm.core.DefaultWatcher;
import io.s4.comm.util.IOUtil;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZkQueue extends DefaultWatcher {
    /**
     * Constructor of producer-consumer queue
     * 
     * @param address
     * @param name
     */
    public ZkQueue(String address, String name) {
        super(address);
        this.root = name;
        // Create ZK node name
        if (zk != null) {
            try {
                Stat s = zk.exists(root, false);
                if (s == null) {
                    zk.create(root,
                              new byte[0],
                              Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
                }
            } catch (KeeperException e) {
                System.out.println("Keeper exception when instantiating queue: "
                        + e.toString());
            } catch (InterruptedException e) {
                System.out.println("Interrupted exception");
            }
        }
    }

    /**
     * Add element to the queue.
     * 
     * @param i
     * @return
     */

    public boolean produce(Object obj) throws KeeperException,
            InterruptedException {
        byte[] value = IOUtil.serializeToBytes(obj);
        zk.create(root + "/element",
                  value,
                  Ids.OPEN_ACL_UNSAFE,
                  CreateMode.PERSISTENT_SEQUENTIAL);
        return true;
    }

    /**
     * Remove first element from the queue.
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public Object consume() throws KeeperException, InterruptedException {
        Object retvalue = -1;
        Stat stat = null;

        // Get the first element available
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(root, true);
                if (list.size() == 0) {
                    System.out.println("Going to wait");
                    mutex.wait();
                } else {
                    Integer min = new Integer(list.get(0).substring(7));
                    String name = list.get(0);
                    for (String s : list) {
                        Integer tempValue = new Integer(s.substring(7));
                        // System.out.println("Temporary value: " + s);
                        if (tempValue < min) {
                            min = tempValue;
                            name = s;
                        }
                    }
                    String zNode = root + "/" + name;
                    System.out.println("Temporary value: " + zNode);
                    byte[] b = zk.getData(zNode, false, stat);
                    zk.delete(zNode, 0);
                    retvalue = IOUtil.deserializeToObject(b);
                    return retvalue;
                }
            }
        }
    }
}
