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
package io.s4.client;

import io.s4.client.util.ByteArrayIOChannel;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * S4 Client Driver.
 * 
 * Allows S4 Client code to send and receive events from an S4 cluster.
 */
public class Driver {
    private static final String protocolName = "generic-json";
    private static final int versionMajor = 1;
    private static final int versionMinor = 0;

    protected String uuid = null;
    protected State state = State.Null;

    protected final String hostname;
    protected final int port;

    protected Socket sock = null;
    protected ByteArrayIOChannel io = null;

    protected ReadMode readMode = ReadMode.Private;
    protected List<String> readInclude = new ArrayList<String>();
    protected List<String> readExclude = new ArrayList<String>();
    protected WriteMode writeMode = WriteMode.Enabled;

    protected boolean debug = false;

    protected int recvTimeoutMs = 0;

    /**
     * Configure driver with Adapter location.
     * 
     * Note: this does not create a connection to the adapter.
     * 
     * @see #init()
     * @see #connect(ReadMode, WriteMode)
     * 
     * @param hostname
     *            Name of S4 client adapter host.
     * @param port
     *            Port on which adapter listens for client connections.
     */
    public Driver(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;
    }

    /**
     * Set the read mode, if not already connected.
     * 
     * @param m
     *            read mode
     * @return Driver with read mode set to {@code m}
     */
    public Driver setReadMode(ReadMode m) {
        if (state != State.Connected)
            this.readMode = m;
        return this;
    }

    /**
     * Add to set of stream names included for reading, if not already
     * connected.
     * 
     * @param s
     *            list of stream names
     * @return Updated driver
     */
    public Driver readInclude(List<String> s) {
        if (state != State.Connected)
            this.readInclude.addAll(s);
        return this;
    }

    /**
     * Add to set of stream names included for reading, if not already
     * connected.
     * 
     * @param s
     *            list of stream names
     * @return Updated driver
     */
    public Driver readInclude(String[] s) {
        return readInclude(Arrays.asList(s));
    }

    /**
     * Add to set of stream names included for reading, if not already
     * connected.
     * 
     * @param s
     *            stream name
     * @return Updated driver
     */
    public Driver readInclude(String s) {
        if (state != State.Connected)
            this.readInclude.add(s);
        return this;
    }

    /**
     * Add to set of stream names excluded from reading, if not already
     * connected.
     * 
     * @param s
     *            list of stream names
     * @return Updated driver
     */
    public Driver readExclude(List<String> s) {
        if (state != State.Connected)
            this.readExclude.addAll(s);
        return this;
    }

    /**
     * Add to set of stream names excluded from reading, if not already
     * connected.
     * 
     * @param s
     *            list of stream names
     * @return Updated driver
     */
    public Driver readExclude(String[] s) {
        return readExclude(Arrays.asList(s));
    }

    /**
     * Add to set of stream names excluded from reading, if not already
     * connected.
     * 
     * @param s
     *            stream name
     * @return Updated driver
     */
    public Driver readExclude(String s) {
        if (state != State.Connected)
            this.readExclude.add(s);
        return this;
    }

    /**
     * Set the write mode, if not already connected.
     * 
     * @param m
     *            write mode
     * @return Driver with write mode set to {@code m}
     */
    public Driver setWriteMode(WriteMode m) {
        if (state != State.Connected)
            this.writeMode = m;
        return this;
    }

    /**
     * 
     * @param debug
     *            debug flag
     * @return Driver with debug flag set to {@code debug}
     */
    public Driver setDebug(boolean debug) {
        this.debug = debug;
        return this;
    }

    /**
     * Set the timeout for receiving data.
     * 
     * @param ms
     *            timeout in milliseconds
     * @return updated driver
     */
    public Driver setRecvTimeout(int ms) {
        this.recvTimeoutMs = ms;
        return this;
    }

    /**
     * Get the state of the driver.
     * 
     * @return state
     */
    public State getState() {
        return state;
    }

    /**
     * Initialize the driver.
     * 
     * Handshake with adapter to receive a unique id, and verify that the driver
     * is compatible with the protocol used by the adapter. This does not
     * actually establish a connection for sending and receiving events.
     * 
     * @see #connect(ReadMode, WriteMode)
     * 
     * @return true if and only if the adapter issued a valid ID to this client,
     *         and the protocol is found to be compatible.
     * @throws IOException
     *             if the underlying TCP/IP socket throws an exception.
     */
    public boolean init() throws IOException {
        if (state.isInitialized())
            return true;

        try {
            sock = new Socket(hostname, port);

            ByteArrayIOChannel io = new ByteArrayIOChannel(sock);

            io.send(emptyBytes);

            byte[] b = io.recv();

            if (b == null || b.length == 0) {
                if (debug) {
                    System.err.println("Empty response during initialization.");
                }
                return false;
            }

            JSONObject json = new JSONObject(new String(b));

            this.uuid = json.getString("uuid");

            JSONObject proto = json.getJSONObject("protocol");

            if (!isCompatible(proto)) {
                if (debug) {
                    System.err
                            .println("Driver not compatible with adapter protocol: "
                                    + proto);
                }
                return false;
            }

            state = State.Initialized;

            return true;

        } catch (JSONException e) {
            if (debug) {
                System.err
                        .println("malformed JSON in initialization response. "
                                + e);
            }
            e.printStackTrace();
            return false;

        } finally {
            sock.close();
        }
    }

    /**
     * Test if the adapter protocol is compatible with this driver. More
     * specifically, the following must be true:
     * {@code
     *     p.name == this.protocolName()
     * AND p.versionMajor == this.versionMajor()
     * AND p.versionMinor >= this.versionMinor()
     * }
     * 
     * @param p
     *            protocol specifier.
     * @return true if and only if the protocol is compatible.
     * @throws JSONException
     *             if some required field was not found in the protocol
     *             specifier.
     */
    private boolean isCompatible(JSONObject p) throws JSONException {
        return p.getString("name").equals(protocolName)
                && (p.getInt("versionMajor") == versionMajor)
                && (p.getInt("versionMinor") >= versionMinor);
    }

    /**
     * Establish a connection to the adapter. Upon success, this enables the
     * client to send and receive events. The client must first be initialized.
     * Otherwise, this operation will fail.
     * 
     * @see #init()
     * 
     * @return true if and only if a connection was successfully established.
     * @throws IOException
     *             if the underlying TCP/IP socket throws an exception.
     */
    public boolean connect() throws IOException {
        if (!state.isInitialized()) {
            // must first be initialized
            if (debug) {
                System.err.println("Not initialized.");
            }
            return false;
        } else if (state.isConnected()) {
            // nothing to do if already connected.
            return true;
        }

        String message = null;

        try {
            // constructing connect message
            JSONObject json = new JSONObject();

            json.put("uuid", uuid);
            json.put("readMode", readMode.toString());
            json.put("writeMode", writeMode.toString());

            if (readInclude != null) {
                // stream inclusion
                json.put("readInclude", new JSONArray(readInclude));
            }

            if (readExclude != null) {
                // stream exclusion
                json.put("readExclude", new JSONArray(readExclude));
            }

            message = json.toString();

        } catch (JSONException e) {
            if (debug) {
                System.err.println("error constructing connect message: " + e);
            }
            return false;
        }

        try {
            // send the message
            this.sock = new Socket(hostname, port);
            this.io = new ByteArrayIOChannel(sock);

            io.send(message.getBytes());

            // get a response
            byte[] b = io.recv();

            if (b == null || b.length == 0) {
                if (debug) {
                    System.err
                            .println("empty response from adapter during connect.");
                }
                return false;
            }

            String response = new String(b);

            JSONObject json = new JSONObject(response);
            String s = json.optString("status", "unknown");

            // does it look OK?
            if (s.equalsIgnoreCase("ok")) {
                // done connecting
                state = State.Connected;
                return true;
            } else if (s.equalsIgnoreCase("failed")) {
                // server has failed the connect attempt
                if (debug) {
                    System.err.println("connect failed by adapter. reason: "
                            + json.optString("reason", "unknown"));
                }
                return false;
            } else {
                // unknown response.
                if (debug) {
                    System.err
                            .println("connect failed by adapter. unrecongnized response: "
                                    + response);
                }
                return false;
            }

        } catch (Exception e) {
            // clean up after error...
            if (debug) {
                System.err.println("error during connect: " + e);
                e.printStackTrace();
            }

            if (this.sock.isConnected()) {
                this.sock.close();

            }

            return false;
        }
    }

    /**
     * Close the connection to the adapter. Events can no longer be sent or
     * received by the client.
     * 
     * @return true upon success. False if the connection is already closed.
     * @throws IOException
     *             if the underlying TCP/IP socket throws an exception.
     */
    public boolean disconnect() throws IOException {
        if (state.isConnected()) {
            io.send(emptyBytes);
            state = State.Null;
            return true;
        }

        return false;
    }

    /**
     * Send a message to the adapter.
     * 
     * @param m
     *            message to be sent
     * @return true if and only if the message was successfully sent.
     * @throws IOException
     *             if the underlying TCP/IP socket throws an exception.
     */

    public boolean send(Message m) throws IOException {

        if (!state.isConnected()) {
            if (debug) {
                System.err.println("send failed. not connected.");
            }
            return false;
        }

        String s = null;

        try {
            JSONObject json = new JSONObject();

            m.toJson(json);

            s = json.toString();

            io.send(s.getBytes());

            return true;

        } catch (JSONException e) {
            if (debug) {
                System.err
                        .println("exception while constructing message to send: "
                                + e);
            }
            return false;
        }
    }

    /**
     * Receive a message from the adapter. This function blocks till a message
     * becomes available to read. The set of messages sent to this client from
     * S4 depends on the read mode of the client.
     * 
     * @see ReadMode
     * 
     * @return message received from S4 cluster.
     * @throws IOException
     *             if the underlying TCP/IP socket throws an exception.
     */
    public Message recv() throws IOException {
        return recv(this.recvTimeoutMs);
    }

    /**
     * Receive a message from the adapter. This function blocks till a message
     * becomes available to read, or till a specified number of milliseconds
     * have elapsed. The set of messages sent to this client from S4 depends on
     * the read mode of the client.
     * 
     * @see ReadMode
     * 
     * @param timeout
     *            timeout in milliseconds
     * @return message received from S4 cluster.
     * @throws IOException
     *             if the underlying TCP/IP socket throws an exception.
     * @throws SocketTimeoutException
     *             if the read timed out.
     */
    public Message recv(int timeout) throws IOException {
        if (!state.isConnected()) {
            if (debug) {
                System.err.println("recv failed. not connected.");
            }
            return null;
        }

        try {
            byte[] b = io.recv(timeout);

            if (b == null || b.length == 0) {
                if (debug) {
                    System.err
                            .println("empty message from adapter. disconnecting");
                }
                this.disconnect();
                return null;
            }

            String s = new String(b);

            JSONObject json = new JSONObject(s);

            return Message.fromJson(json);

        } catch (SocketTimeoutException e) {
            if (debug) {
                System.err.println("recv timed out");
            }
            return null;

        } catch (JSONException e) {
            if (debug) {
                System.err.println("exception while parsing received JSON: "
                        + e);
            }
            return null;
        }
    }

    /**
     * Receive the set of message that from the adapter within a specified time
     * interval.
     * 
     * @param t
     *            interval in milliseconds
     * @return messages received from S4 cluster.
     * @throws IOException
     *             if the underlying TCP/IP socket throws an exception.
     */
    public List<Message> recvAll(int t) throws IOException {
        if (!state.isConnected()) {
            if (debug) {
                System.err.println("recv failed. not connected.");
            }
            return Collections.<Message> emptyList();
        }

        List<Message> messages = new ArrayList<Message>();

        long tStart = System.currentTimeMillis();
        long tEnd = tStart + t;

        long tNow = tStart;

        while (tNow < tEnd) {
            try {
                byte[] b = io.recv((int) (tEnd - tNow));

                if (b == null || b.length == 0) {
                    if (debug) {
                        System.err
                                .println("empty message from adapter. disconnecting");
                    }
                    this.disconnect();
                    break;
                }

                String s = new String(b);

                JSONObject json = new JSONObject(s);

                messages.add(Message.fromJson(json));

            } catch (SocketTimeoutException e) {
                break;

            } catch (JSONException e) {
                if (debug) {
                    System.err
                            .println("exception while parsing received JSON: "
                                    + e);
                }
            }

            tNow = System.currentTimeMillis();
        }

        return messages;
    }

    /**
     * State of the client.
     */
    public enum State {
        /**
         * Uninitialized.
         */
        Null(false, false),

        /**
         * Initialized, but not connected to S4 adapter.
         */
        Initialized(true, false),

        /**
         * Connected to S4 adapter (implies initialized).
         */
        Connected(true, true);

        State(boolean initialized, boolean connected) {
            this.initialized = initialized;
            this.connected = connected;
        }

        private final boolean initialized;
        private final boolean connected;

        /**
         * Is initialization completed?
         * 
         * @return true if and only if this state implies initialization has
         *         been completed.
         */
        public boolean isInitialized() {
            return initialized;
        }

        /**
         * Is client connected to S4 adapter?
         * 
         * @return true if and only if the client is connected to S4 adapter.
         */
        public boolean isConnected() {
            return connected;
        }
    };

    private static final byte[] emptyBytes = {};

    /**
     * Reads and prints all events over an interval of 5 seconds from a set of
     * streams specified on the command line.
     * 
     * @param argv
     *            list of streams
     * @throws IOException
     *             if an error occurs while reading from adapter.
     */
    public static void main(String[] argv) throws IOException {
        Driver d = new Driver("localhost", 2334);

        System.out.println("State: " + d.getState());

        d.init();

        System.out.println("State: " + d.getState());

        d.setReadMode(ReadMode.Select).readInclude(argv)
                .setWriteMode(WriteMode.Enabled);

        d.connect();

        System.out.println("State: " + d.getState());

        List<Message> mm = d.recvAll(5001);
        System.out.println("got messages (" + mm.size() + "): " + mm);

        System.out.println("Disconnecting...");
        d.disconnect();

        System.out.println("State: " + d.getState());
    }
}
