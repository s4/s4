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
import java.util.Map;

import org.apache.log4j.Logger;

public class GenericListener {
    private static Logger logger = Logger.getLogger(GenericListener.class);
    private String zkAddress;
    private DatagramSocket socket;
    int BUFFER_LENGTH = 65507;
    private DatagramPacket dgram;
    private byte[] bs;
    final Deserializer deserializer;

    public GenericListener(String zkaddress, String appName,
            Object listenerConfig) {
        this(zkaddress, appName, listenerConfig, new GenericSerDeser());
    }

    public GenericListener(String zkaddress, String appName,
            Object listenerConfig, Deserializer deserializer) {
        this.zkAddress = zkAddress;
        this.deserializer = deserializer;
        try {
            Map<String, String> map = (Map<String, String>) listenerConfig;
            String mode = map.get("mode");
            int port = Integer.parseInt(map.get("port"));
            if (mode.equals("multicast")) {
                InetAddress inetAddress = InetAddress.getByName(map.get("channel"));
                socket = new MulticastSocket(port);
                ((MulticastSocket) socket).joinGroup(inetAddress);
            }
            if (mode.equals("unicast")) {
                socket = new DatagramSocket(port);
            }
            String udpBufferSize = System.getProperty("udp.buffer.size");
            if (udpBufferSize == null) {
                udpBufferSize = "4194302";
            }
            socket.setReceiveBufferSize(Integer.parseInt(udpBufferSize));
            bs = new byte[BUFFER_LENGTH];
            dgram = new DatagramPacket(bs, bs.length);
        } catch (IOException e) {
            logger.error("error creating listener", e);
            throw new RuntimeException(e);
        }

    }

    public Object receive() {
        try {
            socket.receive(dgram);
            byte[] data = new byte[dgram.getLength()];
            System.arraycopy(dgram.getData(),
                             dgram.getOffset(),
                             data,
                             0,
                             data.length);
            Object object = deserializer.deserialize(data);
            dgram.setLength(BUFFER_LENGTH);
            return object;
        } catch (IOException e) {
            logger.error("error receiving message", e);
            throw new RuntimeException(e);
        }
    }

    /*
     * There is nothing much to do for multicast and unicast
     */
    public void start() {

    }

}
