package io.s4.client;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Messages that can be send/received by client. They typically correspond
 * to events that are sent/received.
 */
class Message {
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