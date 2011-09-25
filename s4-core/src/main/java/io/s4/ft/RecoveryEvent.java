package io.s4.ft;

/**
 * 
 * Event that triggers the recovery of a checkpoint. 
 *
 */
public class RecoveryEvent extends CheckpointingEvent {

    public RecoveryEvent() {
        // as required by default kryo serializer
    }

    public RecoveryEvent(SafeKeeperId safeKeeperId) {
        super(safeKeeperId);
    }

}
