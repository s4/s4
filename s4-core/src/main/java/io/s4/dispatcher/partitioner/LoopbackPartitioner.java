package io.s4.dispatcher.partitioner;

import io.s4.emitter.CommLayerEmitter;
import io.s4.emitter.EventEmitter;
import io.s4.listener.EventListener;
import io.s4.processor.PEContainer;

import java.util.ArrayList;
import java.util.List;

/**
 * A partitioner that assigns events to the current partition, as given by the comm layer.
 * 
 */
public class LoopbackPartitioner implements Partitioner, VariableKeyPartitioner {

    CommLayerEmitter emitter;

    @Override
    public List<CompoundKeyInfo> partition(String streamName,
            List<List<String>> compoundKeyNames, Object event,
            int partitionCount) {
        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();
        CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
        StringBuilder compoundKeyBuilder = new StringBuilder();
        // This partitioning ignores the values of the keyed attributes;
        // it partitions to the current partition id of the pe container
        partitionInfo.setPartitionId(emitter.getListener().getId());
        for (List<String> keyNames : compoundKeyNames) {
            for (String keyName : keyNames) {
                compoundKeyBuilder.append(keyName);
            }
        }
        partitionInfo.setCompoundKey(compoundKeyBuilder.toString());
        partitionInfoList.add(partitionInfo);
        return partitionInfoList;
    }

    @Override
    public List<CompoundKeyInfo> partition(String streamName, Object event,
            int partitionCount) {
        return partition(streamName, new ArrayList<List<String>>(0), event,
                partitionCount);
    }

    /**
     * A reference on the emitter allows getting the current partition id from the comm layer 
     * @param emitter comm layer emitter
     */
    public void setEventEmitter(CommLayerEmitter emitter) {
        this.emitter = emitter;
    }
    
}
