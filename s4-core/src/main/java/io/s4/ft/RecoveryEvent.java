package io.s4.ft;

public class RecoveryEvent extends CheckpointingEvent {

    public RecoveryEvent() {
        // as required by default kryo serializer
    }

    public RecoveryEvent(SafeKeeperId safeKeeperId) {
        super(safeKeeperId);
    }

}
