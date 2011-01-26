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
                System.err.println("Empty response during initialization.");
                return false;
            }

            JSONObject json = new JSONObject(new String(b));

            this.uuid = json.getString("uuid");

            JSONObject proto = json.getJSONObject("protocol");

            if (!isCompatible(proto)) {
                System.err.println("Driver not compatible with adapter protocol: "
                        + proto);
                return false;
            }

            state = State.Initialized;

            return true;

        } catch (JSONException e) {
            System.err.println("malformed JSON in initialization response. "
                    + e);
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
     * @param readMode
     *            Read policy for this client.
     * @param writeMode
     *            Write policy for this client.
     * @return true if and only if a connection was successfully established.
     * @throws IOException
     *             if the underlying TCP/IP socket throws an exception.
     */
    public boolean connect(ReadMode readMode, WriteMode writeMode)
            throws IOException {
        if (!state.isInitialized()) {
            // must first be initialized
            System.err.println("Not initialized.");
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

            message = json.toString();

        } catch (JSONException e) {
            System.err.println("error constructing connect message: " + e);
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
                System.err.println("empty response from adapter during connect.");
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
                System.err.println("connect failed by adapter. reason: "
                        + json.optString("reason", "unknown"));
                return false;
            } else {
                // unknown response.
                System.err.println("connect failed by adapter. unrecongnized response: "
                        + response);
                return false;
            }

        } catch (Exception e) {
            // clean up after error...
            System.err.println("error during connect: " + e);
            e.printStackTrace();

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
            System.err.println("send failed. not connected.");
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
            System.err.println("exception while constructing message to send: "
                    + e);
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

        if (!state.isConnected()) {
            System.err.println("recv failed. not connected.");
            return null;
        }

        byte[] b = io.recv();

        if (b == null || b.length == 0) {
            System.err.println("empty message from adapter. disconnecting");
            this.disconnect();
            return null;
        }

        String s = new String(b);

        try {
            JSONObject json = new JSONObject(s);

            return Message.fromJson(json);

        } catch (JSONException e) {
            System.err.println("exception while parsing received JSON: " + e);
            return null;
        }
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

    /**
     * Messages that can be send/received by client. They typically correspond
     * to events that are sent/received.
     */
    public static class Message {
        public final String stream;
        public final String clazz;
        public final String[] keyNames;
        public final String object;

        /**
         * Key-less event message.
         * 
         * @param stream
         *            Name of stream associated with corresponding event.
         * @param clazz
         *            Class of event object.
         * @param object
         *            String representation of the event. This is the string
         *            that Gson will convert into/from the event object.
         */
        public Message(String stream, String clazz, String object) {
            this.stream = stream;
            this.clazz = clazz;
            this.keyNames = null;
            this.object = object;
        }

        /**
         * Keyed event message.
         * 
         * @param stream
         *            Name of stream associated with corresponding event.
         * @param keyNames
         *            array of key names. Typically, the getter corresponding to
         *            each string in this array will be invoked on the event
         *            object, and the values concatenated, to produce the
         *            routing key.
         * @param clazz
         *            Class of event object.
         * @param object
         *            String representation of the event. This is the string
         *            that Gson will convert into/from the event object.
         */
        public Message(String stream, String[] keyNames, String clazz,
                String object) {
            this.stream = stream;
            this.clazz = clazz;
            this.keyNames = keyNames;
            this.object = object;
        }

        /**
         * Create from JSON.
         * 
         * @param json
         *            JSON representation of message
         * @return Message object.
         * @throws JSONException
         *             if the JSON is invalid.
         */
        public static Message fromJson(JSONObject json) throws JSONException {
            String stream = json.getString("stream");
            String clazz = json.getString("class");
            String object = json.getString("object");

            return new Message(stream, clazz, object);
        }

        /**
         * Convert message into JSON.
         * 
         * @param json
         *            JSON will be written into this object.
         * @throws JSONException
         *             if writing fails.
         */
        public void toJson(JSONObject json) throws JSONException {
            json.put("stream", this.stream);
            json.put("class", this.clazz);
            if (this.keyNames != null) {
                json.put("keyNames", this.keyNames);
            }

            json.put("object", this.object);
        }

        public String toString() {
            return "{stream:" + stream + ", clazz:" + clazz + ", keyNames:"
                    + keyNames + ", object:" + object + "}";
        }
    }

    private static final byte[] emptyBytes = {};

    public static void main(String[] argv) throws IOException {
        Driver d = new Driver("localhost", 2334);
        d.init();

        System.out.println("State: " + d.getState());

        d.connect(ReadMode.All, WriteMode.Disabled);

        System.out.println("State: " + d.getState());

        Message m = d.recv();
        System.out.println("got: " + m);
    }
}
