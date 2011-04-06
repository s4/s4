package io.s4.ft;

// TODO this needs some kind of routing information, provided by safeKeeperId?
public class InitiateCheckpointingEvent extends CheckpointingEvent {

    public InitiateCheckpointingEvent() {
        // as required by default kryo serializer
    }

    public InitiateCheckpointingEvent(SafeKeeperId safeKeeperId) {
        super(safeKeeperId);
    }



}
