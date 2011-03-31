package io.s4.ft;

import io.s4.collector.EventWrapper;
import io.s4.dispatcher.partitioner.CompoundKeyInfo;
import io.s4.emitter.CommLayerEmitter;
import io.s4.schema.Schema;
import io.s4.serialize.KryoSerDeser;
import io.s4.serialize.SerializerDeserializer;
import io.s4.util.LoadGenerator;

import java.util.ArrayList;

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
        Object event = LoadGenerator.makeRecord(jsonRecord,schema);
		EventWrapper eventWrapper = new EventWrapper(streamName,
                      event,
                      new ArrayList<CompoundKeyInfo>());
		eventEmitter.emit(partitionId, eventWrapper);
	}
	
	
	

}
