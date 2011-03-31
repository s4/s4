package io.s4.ft;

public interface StorageCallback {
	
	public void storageOperationResult(SafeKeeper.StorageResultCode resultCode,
			Object message);

}
