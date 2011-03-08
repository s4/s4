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

import io.s4.comm.zk.ZkQueue;

import org.apache.zookeeper.KeeperException;

public class ZkQueueTest {
    public static void main(String args[]) {
        ZkQueue q = new ZkQueue(args[0], "/app1");

        System.out.println("Input: " + args[0]);
        int i;
        Integer max = new Integer(args[1]);

        if (args[2].equals("p")) {
            System.out.println("Producer");
            for (i = 0; i < max; i++)
                try {
                    q.produce(new Integer(10 + i));
                } catch (KeeperException e) {

                } catch (InterruptedException e) {

                }
        } else {
            System.out.println("Consumer");

            for (i = 0; i < max || true; i++) {
                try {
                    Integer r = (Integer) q.consume();
                    System.out.println("Item: " + r);
                } catch (KeeperException e) {
                    e.printStackTrace();
                    i--;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
