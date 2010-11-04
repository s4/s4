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
package io.s4.comm.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class DefaultWatcher implements Watcher {

    public static List<KeeperState> interestingStates = new ArrayList<KeeperState>();
    static {
        interestingStates.add(KeeperState.Expired);
        interestingStates.add(KeeperState.SyncConnected);
    }
    public static Logger logger = Logger.getLogger(DefaultWatcher.class);
    protected ZooKeeper zk = null;
    protected Integer mutex;
    protected String root;
    protected WatchedEvent currentEvent;
    protected CommEventCallback callbackHandler;
    private String zkAddress;
    volatile boolean connected = false;

    protected DefaultWatcher(String address) {
        this(address, null);
    }

    protected DefaultWatcher(String address, CommEventCallback callbackHandler) {
        this.zkAddress = address;
        this.callbackHandler = callbackHandler;
        if (zk == null) {
            try {
                logger.info("Connecting to  zookeeper server:" + address);
                String sTimeout = System.getProperty("zk.session.timeout");
                System.out.println("sTimeout=" + sTimeout);
                int timeout = 30000;
                if (sTimeout != null) {
                    try {
                        timeout = Integer.parseInt(sTimeout);
                    } catch (Exception e) {
                        // ignore will use default
                    }
                }
                mutex = new Integer(-1);
                synchronized (mutex) {
                    zk = new ZooKeeper(address, timeout, this);
                    while (!connected) {
                        logger.info("Waiting for connection to be established ");
                        mutex.wait();
                    }
                }
                logger.info("Connected to zookeeper with sessionid: "
                        + zk.getSessionId() + " and session timeout(ms): "
                        + timeout);

            } catch (Exception e) {
                logger.error("Failed to connect to zookeeper:" + e.getMessage(),
                             e);
                zk = null;
                throw new RuntimeException(e);
            }
        }
    }

    synchronized public void process(WatchedEvent event) {
        logger.info("Received zk event:" + event);
        synchronized (mutex) {
            currentEvent = event;
            if (event.getState() == KeeperState.SyncConnected) {
                connected = true;
            }
            if (callbackHandler != null
                    && interestingStates.contains(event.getState())) {
                Map<String, Object> eventData = new HashMap<String, Object>();
                if (event.getState() == KeeperState.SyncConnected) {
                    eventData.put("state", CommLayerState.INITIALIZED);
                } else if (event.getState() == KeeperState.Expired) {
                    eventData.put("state", CommLayerState.BROKEN);
                }
                eventData.put("source", event);
                callbackHandler.handleCallback(eventData);
            }
            mutex.notify();
        }
    }

    public String getZkAddress() {
        return zkAddress;
    }

}
