package io.s4.ft;

public class InitiateCheckpointingEvent {

	private SafeKeeperId safeKeeperId;

	public InitiateCheckpointingEvent() {
	}

	public InitiateCheckpointingEvent(SafeKeeperId safeKeeperId) {
		super();
		this.safeKeeperId = safeKeeperId;
	}

	public SafeKeeperId getSafeKeeperId() {
		return safeKeeperId;
	}

	public void setSafeKeeperId(SafeKeeperId id) {
		this.safeKeeperId = id;
	}

}
