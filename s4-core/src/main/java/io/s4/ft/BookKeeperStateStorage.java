package io.s4.ft;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * This class implements a BookKeeper backend to persist checkpoints. 
 */

public class BookKeeperStateStorage implements StateStorage {

    private String zkServers;
    private BookKeeper bk;
    static final String PREFIX = "/s4/ckpoints";
    
    BookKeeperStateStorage(String zkServers){
        this.zkServers = zkServers;
    }
    
    public void init(){
        BookKeeper bk = new BookKeeper(zkServers);
     
    }
    
    @Override
    public void saveState(SafeKeeperId key, byte[] state,
            StorageCallback callback) {
        
        /*
         * Creates a new ledger to store the checkpoint
         */
        LedgerHandle lh = bk.createLedger();
     
        /*
         * Write asynchronously to the ledger
         */
        lh.asyncAddEntry(state, this, new ctxObj(key,cb));
        
        /*
         * Record on ZooKeeper the ledger id the key maps to
         */
        bk.getZKHandle().setData(PREFIX + key.toString(), 
                lh.get().toBytes(), 
                -1, 
                this, 
                null);
    }

    @Override
    public byte[] fetchState(SafeKeeperId key) {
        /*
         * Determines ledger id for this key. 
         */
        Stat stat;
        byte[] ledgerIdByte = bk.getZKHandle().getData(PREFIX + key.toString(), 
                false, 
                stat);
        /*
         * TODO: Check if the getData operation has been successful 
         */
        
        /*
         * Open ledger for reading.
         */
        ByteBuffer ledgerIdBB = ByteBuffer.wrap(ledgerIdByte);
        LedgerHandle lh = bk.openLedger(ledgerIdBB.getLong());
        
        /*
         * Read from the ledger
         */
        
        lh.asyncReadEntry(0, this, obj);
        
        /*
         * Wait until it receives a response for the read call 
         * or fails.
         */
        synchronized(obj){        
            while(!obj.set()){ 
                obj.wait(500);
            }
        }
        
        /*
         * Data returns null if operation was unsuccessful.
         */
        return obj.data;
    }

    @Override
    public Set<SafeKeeperId> fetchStoredKeys() {
        /*
         * Get children from zk.
         */
        
        bk.getZKHandle().getChildren(PREFIX, 
                null,
                this,
                CtxObj);
                
        /*
         * Add keys to set.
         */
        Set<SafeKeeperId> set = new Set<SafeKeeperId>();
        for(String s : CtxObj.list){
            set.add(new SafeKeeperId(s));        
        }

        /*
         * Returns an empty set if list is empty
         */
        
        return set;
    }

    /*
     * TODO: Implement all callbacks.
     */
}
