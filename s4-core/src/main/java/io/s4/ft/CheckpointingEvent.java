package io.s4.ft;

/**
 * 
 * <p>
 * This class defines a checkpointing event (either a request for checkpoint or for recovery).
 * </p>
 * <p>
 * Checkpointing events are queued in the PE event queue and processed according the the PE processor scheduler (FIFO).
 * </p>
 */
public abstract class CheckpointingEvent {

    private SafeKeeperId safeKeeperId;

    /**
     * This is a requirement of the serialization framework
     */
    public CheckpointingEvent() {
    }

    /**
     * Constructor identifying the PE subject to checkpointing or recovery
     * @param safeKeeperId safeKeeperId
     */
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
