package io.s4.ft;

/**
 * 
 * Event that triggers a checkpoint.
 *
 */
public class InitiateCheckpointingEvent extends CheckpointingEvent {

    public InitiateCheckpointingEvent() {
        // as required by default kryo serializer
    }

    public InitiateCheckpointingEvent(SafeKeeperId safeKeeperId) {
        super(safeKeeperId);
    }



}
