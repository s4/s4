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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public class GenericSender {
    static Logger logger = Logger.getLogger(GenericSender.class);
    Map<String, String> map;
    private DatagramSocket socket;
    private final String zkAddress;
    ProcessMonitor listenerMonitor;
    int rotationCounter = 0;
    private final Serializer serializer;
    private final int listenerTaskCount;
    private CommEventCallback callbackHandler;
    private String mode;

    public GenericSender(String zkAddress, String appName,
            Object senderConfigData) {
        this(zkAddress, appName, appName, senderConfigData);
    }

    @SuppressWarnings("unchecked")
    public GenericSender(String zkAddress, String adapterClusterName,
            String s4ClusterName, Object senderConfigData, Serializer serializer) {
        this.zkAddress = zkAddress;
        this.serializer = serializer;
        try {
            map = (Map<String, String>) senderConfigData;
            mode = map.get("mode");
            if (mode.equals("multicast")) {
                socket = new MulticastSocket();
            }
            if (mode.equals("unicast")) {
                socket = new DatagramSocket();
            }
            listenerMonitor = CommServiceFactory.getProcessMonitor(this.zkAddress,
                                                                   s4ClusterName,
                                                                   callbackHandler);
            if (callbackHandler != null) {
                // listenerMonitor.setCallbackHandler(callbackHandler);
            }
            listenerMonitor.monitor();
            this.listenerTaskCount = listenerMonitor.getTaskCount();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public GenericSender(String zkAddress, String senderAppName,
            String listenerAppName, Object senderConfigData) {
        this(zkAddress,
             senderAppName,
             listenerAppName,
             senderConfigData,
             new GenericSerDeser());
    }

    /**
     * This method will send the data to receivers in a round robin fashion
     * 
     * @param data
     * @return
     */
    @SuppressWarnings("unchecked")
    public boolean send(Object data) {
        try {
            List<Object> destinationList = listenerMonitor.getDestinationList();
            if (destinationList == null || destinationList.size() == 0) {
                logger.error("Failed to send message: No destination available"
                        + data);
                return false;
            }
            byte[] byteBuffer = serializer.serialize(data);
            rotationCounter = rotationCounter + 1;

            int index = rotationCounter % destinationList.size();
            Map<String, String> dest = (Map<String, String>) destinationList.get(Math.abs(index));
            InetAddress inetAddress;
            int port;
            if (mode.equals("unicast")) {
                inetAddress = InetAddress.getByName(dest.get("address"));
                port = Integer.parseInt((dest.get("port")));
            } else if (mode.equals("multicast")) {
                inetAddress = InetAddress.getByName(dest.get("channel"));
                port = Integer.parseInt((dest.get("port")));
            } else {
                logger.error("Failed to send message unknown mode: " + mode);
                return false;
            }
            DatagramPacket dp = new DatagramPacket(byteBuffer,
                                                   byteBuffer.length,
                                                   inetAddress,
                                                   port);
            socket.send(dp);
        } catch (IOException e) {
            // add retry
            logger.error("Failed to send message: " + data, e);
            return false;
        }
        return true;
    }

    /**
     * This will send the data to a specific channel/receiver/partition
     * 
     * @param partition
     * @param data
     * @return
     */
    @SuppressWarnings("unchecked")
    public boolean sendToPartition(int partition, Object data) {
        try {
            byte[] byteBuffer = serializer.serialize(data);
            Map<Integer, Object> destinationMap = listenerMonitor.getDestinationMap();
            if (logger.isDebugEnabled()) {
                logger.debug("Destination Map:" + destinationMap);
            }
            Map<String, String> dest = (Map<String, String>) destinationMap.get(partition);
            if (dest != null) {
                InetAddress inetAddress = InetAddress.getByName(dest.get("address"));
                int port = Integer.parseInt((dest.get("port")));
                DatagramPacket dp = new DatagramPacket(byteBuffer,
                                                       byteBuffer.length,
                                                       inetAddress,
                                                       port);
                socket.send(dp);
            } else {
                logger.warn("Destination not available for partition:"
                        + partition + " Skipping message:" + data);
                return false;
            }
        } catch (IOException e) {
            // add retry
            logger.error("Failed to send message: " + data, e);
            return false;
        }

        return true;

    }

    /**
     * compute partition using hashcode and send to appropriate partition
     * 
     * @param partition
     * @param data
     * @return
     */
    public boolean sendUsingHashCode(int hashcode, Object data) {
        int partition = (hashcode & Integer.MAX_VALUE) % listenerTaskCount;
        return sendToPartition(partition, data);

    }

    public CommEventCallback getCallbackHandler() {
        return callbackHandler;
    }

    public void setCallbackHandler(CommEventCallback callbackHandler) {
        this.callbackHandler = callbackHandler;
    }

    public int getListenerTaskCount() {
        return listenerTaskCount;
    }

}
