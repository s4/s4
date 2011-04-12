package io.s4.ft;

// checkpointing events require some info for routing, that we may extract from safekeeper id
public abstract class CheckpointingEvent {

    private SafeKeeperId safeKeeperId;

    public CheckpointingEvent() {
    }

    public CheckpointingEvent(SafeKeeperId safeKeeperId) {
        this.safeKeeperId = safeKeeperId;
    }

    public SafeKeeperId getSafeKeeperId() {
        return safeKeeperId;
    }

    public void setSafeKeeperId(SafeKeeperId id) {
        this.safeKeeperId = id;
    }

}
