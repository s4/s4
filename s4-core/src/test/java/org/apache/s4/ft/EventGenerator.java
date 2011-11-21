package org.apache.s4.ft;

import org.apache.s4.collector.EventWrapper;
import org.apache.s4.dispatcher.partitioner.CompoundKeyInfo;
import org.apache.s4.dispatcher.partitioner.KeyInfo;
import org.apache.s4.emitter.CommLayerEmitter;
import org.apache.s4.schema.Schema;
import org.apache.s4.serialize.KryoSerDeser;
import org.apache.s4.serialize.SerializerDeserializer;
import org.apache.s4.util.LoadGenerator;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

public class EventGenerator {

    private CommLayerEmitter eventEmitter;

    public EventGenerator() {
        SerializerDeserializer serDeser = new KryoSerDeser();

        eventEmitter = new CommLayerEmitter();
        eventEmitter.setAppName("s4");
        eventEmitter.setListenerAppName("s4");
        eventEmitter.setClusterManagerAddress("localhost");
        eventEmitter
                .setSenderId(String.valueOf(System.currentTimeMillis() / 1000));
        eventEmitter.setSerDeser(serDeser);
        eventEmitter.init();

        LoadGenerator generator = new LoadGenerator();
        generator.setEventEmitter(eventEmitter);
    }

    public void injectValueEvent(KeyValue keyValue, String streamName,
            int partitionId) throws JSONException {

        Schema schema = new Schema(KeyValue.class);
        JSONObject jsonRecord = new JSONObject("{key:" + keyValue.getKey()
                + ",value:" + keyValue.getValue() + "}");
        Object event = LoadGenerator.makeRecord(jsonRecord, schema);
        CompoundKeyInfo compoundKeyInfo = new CompoundKeyInfo();
        compoundKeyInfo.setCompoundKey("key");
        compoundKeyInfo.setCompoundValue("value");
        List<CompoundKeyInfo> compoundKeyInfos = new ArrayList<CompoundKeyInfo>();
        compoundKeyInfos.add(compoundKeyInfo);
        EventWrapper eventWrapper = new EventWrapper(streamName, event,
                compoundKeyInfos);
        eventEmitter.emit(partitionId, eventWrapper);
    }

    public void injectEvent(Object event, String streamName, int partitionId,
            List<CompoundKeyInfo> compoundKeyInfos) throws JSONException {

        EventWrapper eventWrapper = new EventWrapper(streamName, event,
                compoundKeyInfos);
        eventEmitter.emit(partitionId, eventWrapper);
    }

}
