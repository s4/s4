/*
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 	        http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package io.s4.ft;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

/**
 * <p>
 * Implementation of a file system backend storage to persist checkpoints.
 * </p>
 * <p>
 * The file system may be the default local file system when running on a single
 * machine, but should be a distributed file system such as NFS when running on
 * a cluster.
 * </p>
 * <p>
 * Checkpoints are stored in individual files (1 file = 1 safeKeeperId) in
 * directories according to the following structure:
 * <code>(storageRootpath)/prototypeId/safeKeeperId</code>
 * </p>
 * 
 */
public class DefaultFileSystemStateStorage implements StateStorage {

    private static org.apache.log4j.Logger logger = Logger.getLogger(DefaultFileSystemStateStorage.class);
    private String storageRootPath;

    public DefaultFileSystemStateStorage() {
    }

    /**
     * <p>
     * Called by the dependency injection framework, after construction.
     * <p/>
     */
    public void init() {
        checkStorageDir();
    }

    @Override
    public byte[] fetchState(SafeKeeperId key) {
        File file = safeKeeperID2File(key, storageRootPath);
        if (file != null && file.exists()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Fetching " + file.getAbsolutePath() + "for : " + key);
            }
            // TODO use commons-io or guava
            FileInputStream in = null;
            try {
                in = new FileInputStream(file);

                long length = file.length();

                /*
                 * Arrays can only be created using int types, so ensure that
                 * the file size is not too big before we downcast to create the
                 * array.
                 */
                if (length > Integer.MAX_VALUE) {
                    throw new IOException("Error file is too large: " + file.getName() + " " + length + " bytes");
                }

                byte[] buffer = new byte[(int) length];
                int offSet = 0;
                int numRead = 0;

                while (offSet < buffer.length && (numRead = in.read(buffer, offSet, buffer.length - offSet)) >= 0) {
                    offSet += numRead;
                }

                if (offSet < buffer.length) {
                    throw new IOException("Error, could not read entire file: " + file.getName() + " " + offSet + "/"
                            + buffer.length + " bytes read");
                }

                in.close();
                return buffer;
            } catch (FileNotFoundException e1) {
                logger.error(e1.getMessage(), e1);
            } catch (IOException e2) {
                logger.error(e2.getMessage(), e2);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (Exception e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
        return null;

    }

    @Override
    public Set<SafeKeeperId> fetchStoredKeys() {
        Set<SafeKeeperId> keys = new HashSet<SafeKeeperId>();
        File rootDir = new File(storageRootPath);
        File[] dirs = rootDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        for (File dir : dirs) {
            File[] files = dir.listFiles(new FileFilter() {
                @Override
                public boolean accept(File file) {
                    return (file.isFile());
                }
            });
            for (File file : files) {
                keys.add(file2SafeKeeperID(file));
            }
        }
        return keys;
    }

    // files kept as : root/<prototypeId>/encodedKeyWithFullInfo
    private static File safeKeeperID2File(SafeKeeperId key, String storageRootPath) {

        return new File(storageRootPath + File.separator + key.getPrototypeId() + File.separator
                + Base64.encodeBase64URLSafeString(key.getStringRepresentation().getBytes()));
    }

    private static SafeKeeperId file2SafeKeeperID(File file) {
        SafeKeeperId id = null;
        id = new SafeKeeperId(new String(Base64.decodeBase64(file.getName())));
        return id;
    }

    public String getStorageRootPath() {
        return storageRootPath;
    }

    public void setStorageRootPath(String storageRootPath) {
        this.storageRootPath = storageRootPath;
        File rootPathFile = new File(storageRootPath);
        if (!rootPathFile.exists()) {
            if (!rootPathFile.mkdirs()) {
                logger.error("could not create root storage directory : " + storageRootPath);
            }

        }
    }

    public void checkStorageDir() {
        if (storageRootPath == null) {

            File defaultStorageDir = new File(System.getProperty("user.dir") + File.separator + "tmp" + File.separator
                    + "storage");
            storageRootPath = defaultStorageDir.getAbsolutePath();
            if (logger.isInfoEnabled()) {
                logger.info("Unspecified storage dir; using default dir: " + defaultStorageDir.getAbsolutePath());
            }
            if (!defaultStorageDir.exists()) {
                if (!(defaultStorageDir.mkdirs())) {
                    logger.error("Storage directory not specified, and cannot create default storage directory : "
                            + defaultStorageDir.getAbsolutePath() + "\n Checkpointing and recovery will be disabled.");
                }
            }
        }
    }

    @Override
    public void saveState(SafeKeeperId key, byte[] state, StorageCallback callback) {
        File f = safeKeeperID2File(key, storageRootPath);
        if (logger.isDebugEnabled()) {
            logger.debug("Checkpointing [" + key + "] into file: [" + f.getAbsolutePath() + "]");
        }
        if (!f.exists()) {
            if (!f.getParentFile().exists()) {
                // parent file has prototype id
                if (!f.getParentFile().mkdir()) {
                    callback.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE,
                            "Cannot create directory for storing PE [" + key.toString() + "] for prototype: "
                                    + f.getParentFile().getAbsolutePath());
                    return ;
                }
            }
            // TODO handle IO exception
            try {
                f.createNewFile();
            } catch (IOException e) {
                callback.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE,
                        key.toString() + " : " + e.getMessage());
                return ;
            }
        } else {
            if (!f.delete()) {
                callback.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE,
                        "Cannot delete previously saved checkpoint file [" + f.getParentFile().getAbsolutePath() + "]");
                return ;
            }
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(f);
            fos.write(state);
            callback.storageOperationResult(SafeKeeper.StorageResultCode.SUCCESS, key.toString());
        } catch (FileNotFoundException e) {
            callback.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE,
                    key.toString() + " : " + e.getMessage());
        } catch (IOException e) {
            callback.storageOperationResult(SafeKeeper.StorageResultCode.FAILURE,
                    key.toString() + " : " + e.getMessage());
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

}
