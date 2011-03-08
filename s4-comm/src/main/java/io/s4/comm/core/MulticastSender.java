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

import io.s4.comm.util.IOUtil;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Map;

public class MulticastSender {

    Map<String, String> map;
    private MulticastSocket ms;
    private InetAddress inetAddress;
    private int port;

    @SuppressWarnings("unchecked")
    public MulticastSender(Object senderConfigData) {
        try {
            map = (Map<String, String>) senderConfigData;
            ms = new MulticastSocket();
            inetAddress = InetAddress.getByName(map.get("multicast.address"));
            ms.joinGroup(inetAddress);
            this.port = Integer.parseInt(map.get("multicast.port"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This method will send the data to receivers in a round robin fashion
     * 
     * @param data
     * @return
     */
    public boolean send(Object data) {
        try {
            byte[] byteBuffer = IOUtil.serializeToBytes(data);
            DatagramPacket dp = new DatagramPacket(byteBuffer,
                                                   byteBuffer.length,
                                                   inetAddress,
                                                   port);
            ms.send(dp);
        } catch (IOException e) {
            // add retry
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * This will send the data to a specific channel/receiver
     * 
     * @param partition
     * @param data
     * @return
     */
    public boolean send(int partition, Object data) {
        return true;
    }

}
