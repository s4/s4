package io.s4.ft;

import java.util.Set;

public interface StateStorage {
	
	
	public void saveState(SafeKeeperId key, byte[] state,
			StorageCallback callback);

	public byte[] fetchState(SafeKeeperId key);

    // returns empty if no stored key
	public Set<SafeKeeperId> fetchStoredKeys();

}
