package io.s4.dispatcher.partitioner;

import io.s4.processor.PEContainer;

import java.util.ArrayList;
import java.util.List;

public class LoopbackPartitioner implements Partitioner, VariableKeyPartitioner {

    private PEContainer peContainer;

    @Override
    public List<CompoundKeyInfo> partition(String streamName,
            List<List<String>> compoundKeyNames, Object event,
            int partitionCount) {
        List<CompoundKeyInfo> partitionInfoList = new ArrayList<CompoundKeyInfo>();
        CompoundKeyInfo partitionInfo = new CompoundKeyInfo();
        StringBuilder compoundKeyBuilder = new StringBuilder();
        // This partitioning ignores the values of the keyed attributes;
        // it partitions to the current partition id of the pe container
        partitionInfo.setPartitionId(peContainer.getPartitionId());
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

    public void setPeContainer(PEContainer peContainer) {
        this.peContainer = peContainer;
    }

    public PEContainer getPeContainer() {
        return peContainer;
    }

}
