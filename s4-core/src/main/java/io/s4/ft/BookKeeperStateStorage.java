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

import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CreateCallback;
import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * <p>
 * This class implements a BookKeeper backend to persist checkpoints.
 * </p>
 * <p>
 * BookKeeper is a distributed logging service
 * </p>
 * <p> 
 * See {@link http://zookeeper.apache.org/bookkeeper. BookKeeper Apache's page} for more information on BookKeeper.
 * </p>
 * <p>
 * BookKeeper must be configured as an external service. References to this external service are injected
 * into this backend from the core configuration.
 * </p>
 */
public class BookKeeperStateStorage 
implements StateStorage, 
DataCallback, 
ChildrenCallback, 
CreateCallback, 
OpenCallback,
//DeleteCallback,
AddCallback,
ReadCallback, StatCallback, DeleteCallback, org.apache.zookeeper.AsyncCallback.StringCallback {
    private static Logger logger = Logger.getLogger("s4-ft");

    private String zkServers;
    private BookKeeper bk;
    static final String PREFIX = "/s4/checkpoints";
    private int ensembleSize = 4;
    private int quorumSize = 2;
    
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
            this.keys = null;
        }
    }
    
    class LedgerIdMappingCtx {
        long ledgerId;
        SafeKeeperId safeKeeperId;
        
        LedgerIdMappingCtx(long ledgerId, SafeKeeperId safeKeeperId) {
            this.ledgerId = ledgerId;
            this.safeKeeperId = safeKeeperId;
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
        
        synchronized void block()
        throws InterruptedException {
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
    public BookKeeperStateStorage() {
    }
    
    /**
     * Initializes storage object.
     */
    public void init() throws Throwable{
        this.bk = new BookKeeper(zkServers);
        logger.info("Initialized BookKeeper storage backend");
    }
    
    /**
     * Call to queue request to save state. Part of the 
     * StateStorage interface definition.
     */
    @Override
    public void saveState(SafeKeeperId key, byte[] state,
            StorageCallback callback) {
        if (logger.isDebugEnabled()) {
            logger.debug("checkpointing: " + key);
        }
        SaveCtx sctx = new SaveCtx(key, state, callback);
        
        // TODO
        // if ledger exist for this prototype, use it, else fetch it from zookeeper, else create it
        // then write entry to ledger
        // then add entry id to index ledger
        
        /*
         * Creates a new ledger to store the checkpoint
         */
        bk.asyncCreateLedger(ensembleSize,quorumSize, 
                DigestType.CRC32, 
                "flaviowashere".getBytes(), 
                (AsyncCallback.CreateCallback) this, 
                (Object) sctx);
        
    }
    
    
    @Override
    public byte[] fetchState(SafeKeeperId key) {
        SmallBarrier sb = new SmallBarrier();
        FetchCtx fctx = new FetchCtx(key, sb);
         asyncFetchState(key, fctx);
        /*
         * Wait until it receives a response for the read call 
         * or fails.
         * TODO: Not very elegant to catch the exception here, but
         *  the interface does not accept it currently.
         */
        try{
            sb.block();
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for state.", e);
            return null;
        }
    
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
    public void asyncFetchState(SafeKeeperId key, FetchCtx fctx) {
        /*
         * Determines ledger id for this key. 
         */
        bk.getZkHandle().getData(safeKeeperId2BKEntryPath(key), null, this, fctx);
    }

    @Override
    public Set<SafeKeeperId> fetchStoredKeys() {
        SmallBarrier sb = new SmallBarrier();
        FetchKeysCtx ctx = new FetchKeysCtx(sb);
        if (logger.isDebugEnabled()) {
            logger.debug("fetching stored keys");
        }
        /*
         * Asynchronous call to fetch keys.
         */
        asyncFetchStoredKeys(ctx);
        
        /*
         * Blocks until operation completes.
         */
        try{
            sb.block();
        } catch (InterruptedException e) {
            logger.warn("Interrupted while waiting for keys.", e);
            return null;
        }
        /*
         * Returns an empty set if list is empty
         */
        
        if (ctx.keys == null) {
            return new HashSet<SafeKeeperId>(0);
        } else {
            return ctx.keys;
        }
    }
    
    /**
     * Fetches stored keys asynchronously.
     */
    public void asyncFetchStoredKeys(FetchKeysCtx ctx) {
        /*
         * Get children from zk.
         */
        bk.getZkHandle().getChildren(PREFIX, 
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
    public void createComplete(int rc, LedgerHandle lh, Object ctx){
        SaveCtx sctx = (SaveCtx) ctx;
        
        if(rc == BKException.Code.OK){
            lh.asyncAddEntry(sctx.state, this, sctx);
            if(logger.isDebugEnabled()) {
                logger.debug("created ledger entry ["+lh.getId() +"] for key ["+sctx.key+"]");
            }
            
        } else {
            logger.error("Failed to create a ledger to store checkpoint: " + rc);
            /*
             * Request failed, so have to call back if object is not null.
             */
            if(sctx.cb != null){
                sctx.cb.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE, BKException.getMessage(rc));
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
        lh.asyncReadEntries(0, 0, (AsyncCallback.ReadCallback) this, (Object) ctx);
    }
    
    /**
     * BookKeeper read callback.
     * 
     * @param rc
     * @param lh
     * @param seq
     * @param ctx
     */
    public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq,
            Object ctx){
        FetchCtx fctx = (FetchCtx) ctx;
        if(rc == BKException.Code.OK) {
            fctx.state = seq.nextElement().getEntry();
            
        } else {
            logger.error("Reading checkpoint failed : " + rc);
        }
        
        if(fctx.sb != null){
            fctx.sb.cross();
        }
        
    }
    
    /**
     * Add complete.
     */
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx){
        SaveCtx sctx = (SaveCtx) ctx;
        
        if(rc == BKException.Code.OK) {
            /*
             * Record on ZooKeeper the ledger id the key maps to
             */
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong(lh.getId());
            try {
                try {
                    byte[] oldData =bk.getZkHandle().getData(safeKeeperId2BKEntryPath(sctx.key), null, null); 
                    
                    bk.getZkHandle().setData(safeKeeperId2BKEntryPath(sctx.key), 
                            bb.array(), 
                            -1, 
                            (StatCallback) this, 
                            new LedgerIdMappingCtx(lh.getId(), sctx.key));
                    // remove old ledger
                    long oldLedgerId = ByteBuffer.wrap(oldData).getLong();
                    bk.asyncDeleteLedger(oldLedgerId, this, new LedgerIdMappingCtx(oldLedgerId, sctx.key));
                } catch (NoNodeException nne) {
                    bk.getZkHandle().create(safeKeeperId2BKEntryPath(sctx.key), bb.array(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, this, sctx);
                }
                if(sctx.cb != null){
                    sctx.cb.storageOperationResult(SafeKeeper.StorageResultCode.SUCCESS, BKException.getMessage(rc));
                }
            } catch (KeeperException e) {
                logger.error("Could not write checkpoint " + sctx.key, e);
                if(sctx.cb != null){
                    sctx.cb.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE, e.getMessage());
                }
            } catch (InterruptedException e) {
                logger.error("Could not write checkpoint " + sctx.key, e);
                if(sctx.cb != null){
                    sctx.cb.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE, e.getMessage());
                }
            }
            
        } else {
            logger.error("Failed to write state to BookKeeper: " + sctx.key + " : " + rc);
            if(sctx.cb != null){
                sctx.cb.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE, BKException.getMessage(rc));
            }
        }
    }
    
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if(logger.isDebugEnabled()) {
            LedgerIdMappingCtx mapping = (LedgerIdMappingCtx)ctx;
            logger.debug("checkpoint replacement: created ledger ["+mapping.ledgerId +"] for key ["+mapping.safeKeeperId+"]");
        }

    }

    
    /**
     * ZooKeeper data callback.
     * 
     * See {@link DataCallback#processResult(int, String, Object, byte[], Stat)}
     */
    public void processResult(int rc, String path, Object ctx, byte data[],
            Stat stat){
        FetchCtx fctx = (FetchCtx) ctx;
        
        if(KeeperException.Code.get(rc) == KeeperException.Code.OK){
            /*
             * Open ledger for reading.
             */
            ByteBuffer ledgerIdBB = ByteBuffer.wrap(data);
            bk.asyncOpenLedger(ledgerIdBB.getLong(), 
                    DigestType.CRC32, 
                    "flaviowashere".getBytes(),
                    (AsyncCallback.OpenCallback) this,
                    (Object) fctx);
        } else {
            logger.error("Failed to open ledger for reading: " + rc);
            if (fctx.sb!=null) {
                fctx.sb.cross();
            }
        }
    }
    
    /**
     * ZooKeeper get children callback.
     * 
     * See {@link ChildrenCallback#processResult(int, String, Object, List)}
     */
    public void processResult(int rc, String path, Object ctx,
            List<String> children){
        FetchKeysCtx fkCtx = (FetchKeysCtx) ctx;
        if(KeeperException.Code.get(rc) == KeeperException.Code.OK){
            /*
             * Add keys to set.
             */
            Set<SafeKeeperId> fetchedKeys = new HashSet<SafeKeeperId>();
            for(String child : children){
                fetchedKeys.add(bKEntryPath2SafeKeeperId(child));        
            }
            fkCtx.keys = fetchedKeys;
        } else {
            logger.error("Failed to get keys from ZooKeeper: " + rc);
        }
        
        if(fkCtx.sb != null){
            fkCtx.sb.cross();
        } 
    }
    
    /**
     * Callback from {@link org.apache.zookeeper.AsyncCallback.StringCallback}
     */
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        if (logger.isDebugEnabled()) {
            SaveCtx sctx = (SaveCtx)ctx;
            logger.debug("Added ledger mapping for safeKeeperId [" + sctx.key+"]");
        }
        
    }

    

    /**
     * Callback from {@link AsyncCallback.DeleteCallback}
     */
    @Override
    public void deleteComplete(int arg0, Object arg1) {
        if(logger.isDebugEnabled()) {
            LedgerIdMappingCtx ctx = (LedgerIdMappingCtx)arg1;
            logger.debug("Deleted ledger with id ["+ctx.ledgerId+"] " +
                    "for safeKeeperId ["+ctx.safeKeeperId);
        }
    }


    public void setZkServers(String zkServers) {
        this.zkServers = zkServers;
    }
    
    
    public String safeKeeperId2BKEntryPath(SafeKeeperId safeKeeperId) {
        return PREFIX+"/"+Base64.encodeBase64URLSafeString(safeKeeperId.getStringRepresentation()
                .getBytes());
    }
    
    public SafeKeeperId bKEntryPath2SafeKeeperId(String suffix) {
        return new SafeKeeperId(PREFIX+"/"+new String(Base64.decodeBase64(suffix)));
    }

    public int getEnsembleSize() {
        return ensembleSize;
    }

    public void setEnsembleSize(int ensembleSize) {
        this.ensembleSize = ensembleSize;
    }

    public int getQuorumSize() {
        return quorumSize;
    }

    public void setQuorumSize(int quorumSize) {
        this.quorumSize = quorumSize;
    }


    
    

}
