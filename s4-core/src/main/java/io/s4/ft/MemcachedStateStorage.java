package io.s4.ft;

import java.util.Set;

public class MemcachedStateStorage implements StateStorage {

    @Override
    public void saveState(SafeKeeperId key, byte[] state,
            StorageCallback callback) {
        // TODO Auto-generated method stub

    }

    @Override
    public byte[] fetchState(SafeKeeperId key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<SafeKeeperId> fetchStoredKeys() {
        // TODO Auto-generated method stub
        return null;
    }

}
