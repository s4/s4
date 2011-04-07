/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *          http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */

package io.s4.ft;

import io.s4.listener.CommLayerListener;

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;

/**
 * This class implements a BookKeeper backend to persist checkpoints. 
 */

public class BookKeeperStateStorage 
implements StateStorage, 
DataCallback, 
ChildrenCallback, 
CreateCallback, 
OpenCallback,
DeleteCallback,
AddCallback,
ReadCallback {
    private static Logger logger = Logger.getLogger(BookKeeperStateStorage.class);

    private String zkServers;
    private BookKeeper bk;
    static final String PREFIX = "/s4/checkpoints";
    
    /*
     * Context object for saving checkpoints.
     */
    class SaveCtx {
        SafeKeeperId key;
        byte[] state;
        StorageCallback cb;
        
        SaveCtx(SafeKeeperId key, byte[] state, StorageCallback cb){
            this.key = key;
            this.state = state;
            this.cb = cb;
        }
    }
    
    /*
     * Context objects for fetching checkpoints.
     */
    class FetchCtx {
        SafeKeeperId key;
        byte[] state;
        SmallBarrier sb;
        
        FetchCtx(SafeKeeperId key, SmallBarrier sb){
         this.key = key;
         this.state = null;
         this.sb = sb;
        }
    }
    
    /*
     * Context objects for fetching existing keys.
     */
    class FetchKeysCtx {
        SmallBarrier sb;
        Set<SafeKeeperId> keys;
        
        FetchKeysCtx(SmallBarrier sb){
            this.sb = sb;
            this.set = null;
        }
    }
    
    /*
     * Control objects to wait until operations complete.
     */
    class SmallBarrier {
        boolean released;
        List<String> list;
        byte[] data;
        
        SmallBarrier(){
            this.released = false;
        }
        
        synchronized void block(){
            if(released) return;
            else wait();    
        }
                
        synchronized void cross(){
            released = true;
            notify();
        }
        
        synchronized void reset() {
            released = false;
        }
    }
    
    /**
     * Constructor
     * @param zkServers
     */
    BookKeeperStateStorage(String zkServers){
        this.zkServers = zkServers;
    }
    
    /**
     * Initializes storage object.
     */
    public void init(){
        BookKeeper bk = new BookKeeper(zkServers);
    }
    
    /**
     * Call to queue request to save state. Part of the 
     * StateStorage interface definition.
     */
    @Override
    public void saveState(SafeKeeperId key, byte[] state,
            StorageCallback callback) {
        
        SaveCtx sctx = new SaveCtx(key, state, callback);
        /*
         * Creates a new ledger to store the checkpoint
         */
        LedgerHandle lh = bk.asyncCreateLedger(this, sctx);
        
    }
    
    
    @Override
    public byte[] fetchState(SafeKeeperId key) {
        SmallBarrier sb = new SmallBarrier();
        FetchCtx fctx = new FetchCtx(key, sb);
        asyncFetchState(key, fctx);
        /*
         * Wait until it receives a response for the read call 
         * or fails.
         */
        sb.block();
    
        /*
         * Data returns null if operation was unsuccessful.
         */
        return fctx.state;
    }
    
    /**
     * Asynchronous call to fetch state. Includes a small bar
     * @param key
     * @param sb
     * @return
     */
    public byte[] asyncFetchState(SafeKeeperId key, FetchCtx fctx) {
        /*
         * Determines ledger id for this key. 
         */
        Stat stat;
        byte[] ledgerIdByte = bk.getZKHandle().getData(PREFIX + key.toString(), 
                null,
                this,
                fctx);
    }
        
    @Override
    public Set<SafeKeeperId> fetchStoredKeys() {
        SmallBarrier sb = new SmallBarrier();
        FetchKeyCtx ctx = new FetchKeysCtx(sb);
        
        /*
         * Asynchronous call to fetch keys.
         */
        asyncFetchStoredKeys(ctx);
        
        /*
         * Blocks until operation completes.
         */
        sb.block();
        
        /*
         * Returns an empty set if list is empty
         */
        
        return fkCtx.set;
    }
    
    /**
     * Fetches stored keys asynchronously.
     */
    public void asyncFetchStoredKeys(FetchKeysCtx ctx) {
        /*
         * Get children from zk.
         */
        SmallBarrier sb = new SmallBarrier();
        bk.getZKHandle().getChildren(PREFIX, 
                null,
                this,
                ctx);
    }

    /**
     * BookKeeper create callback.
     * 
     * @param rc
     * @param lh
     * @param ctx
     */
    void createComplete(int rc, LedgerHandle lh, Object ctx){
        SaveCtx sctx = (SaveCtx) ctx;
        
        if(rc == BKException.Code.OK){
            lh.asyncAddEntry(sctx.state, this, sctx);
        } else {
            logger.error("Failed to create a ledger to store checkpoint: " + rc);
            /*
             * Request failed, so have to call back if object is not null.
             */
            if(sctx.cb != null){
                sctx.cb(SafeKeeper.StorageResultCode.FAILURE, BKException.getMessage(rc));
            }
        }
    }
    
    /**
     * BookKeeper open ccallback.
     * 
     * @param rc
     * @param lh
     * @param ctx
     */
    public void openComplete(int rc, LedgerHandle lh, Object ctx){
        /*
         * Read from the ledger
         */
        lh.asyncReadEntry(0, this, sb);
    }
    
    /**
     * BookKeeper read callback.
     * 
     * @param rc
     * @param lh
     * @param seq
     * @param ctx
     */
    void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq,
            Object ctx){
        FetchCtx fctx = (FetchCtx) ctx;
        if(rc == BKException.Code.OK) {
            fctx.state = seq.nextElement();
            
        } else {
            logger.error("Reading checkpoint failed.");
        }
        
        if(fctx.sb != null){
            fctx.sb.cross();
        }
        
    }
    
    void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx){
        SaveCtx sctx = (SaveCtx) ctx;
        
        if(rc == BKException.Code.OK) {
            /*
             * Record on ZooKeeper the ledger id the key maps to
             */
            bk.getZKHandle().setData(PREFIX + sctx.key.toString(), 
                    lh.get().toBytes(), 
                    -1, 
                    this, 
                    null);
            if(sctx.cb != null){
                sctx.cb(SafeKeeper.StorageResultCode.SUCCESS, BKException.getMessage(rc));
            }
            
        } else {
            logger.error("Failed to write state to BookKeeper: " + rc);
            if(sctx.cb != null){
                sctx.cb(SafeKeeper.StorageResultCode.FAILURE, BKException.getMessage(rc));
            }
        }
        
        /*
         * If callback is not null, have to return, independent of result
         * TODO: Complete callback
         */
        
    }
    
    /**
     * ZooKeeper data callback.
     * 
     * @param rc
     * @param path
     * @param ctx
     * @param data
     * @param stat
     */
    public void processResult(int rc, String path, Object ctx, byte data[],
            Stat stat){
        FetchCtx fctx = (FetchCtx) ctx;
        
        if(KeeperException.Code.get(rc) == KeeperException.Code.OK){
            /*
             * Open ledger for reading.
             */
            ByteBuffer ledgerIdBB = ByteBuffer.wrap(data);
            bk.asyncOpenLedger(ledgerIdBB.getLong());
        } else {
            logger.error("Failed to open ledger for reading: " + rc);
        }
    }
    
    /**
     * ZooKeeper get children callback.
     * 
     * @param rc
     * @param path
     * @param ctx
     * @param children
     */
    public void processResult(int rc, String path, Object ctx,
            List<String> children){
        FetchKeysCtx fkCtx = (FetchKeysCtx) ctx;
        
        if(KeeperException.Code.get(rc) == KeeperException.Code.OK){
            /*
             * Add keys to set.
             */
            Set<SafeKeeperId> set = new HashSet<SafeKeeperId>();
            for(String s : children){
                fkCtx.keys.add(new SafeKeeperId(s));        
            }
            fkCtx.keys = set;
        } else {
            logger.error("Failed to get keys from ZooKeeper: " + rc);
        }
        
        if(fkCtx.sb != null){
            fkCtx.sb.cross();
        } 
    }
}
