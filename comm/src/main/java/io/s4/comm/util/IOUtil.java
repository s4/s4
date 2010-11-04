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
package io.s4.comm.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class IOUtil {

    public static void save(Object obj, String path) throws Exception {
        File f = new File(path);
        FileOutputStream fos = new FileOutputStream(f);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fos);
        objectOutputStream.writeObject(obj);
        objectOutputStream.close();
    }

    public static Object read(String path) throws Exception {
        File f = new File(path);
        FileInputStream fos = new FileInputStream(f);
        ObjectInputStream objectInputStream = new ObjectInputStream(fos);
        Object readObject = objectInputStream.readObject();
        objectInputStream.close();
        return readObject;
    }

    public static byte[] serializeToBytes(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(bos);
            objectOutputStream.writeObject(obj);
            objectOutputStream.close();
            return bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Exception trying to serialize to bytes, obj="
                                               + obj,
                                       e);
        }
    }

    public static Object deserializeToObject(byte[] bytes) {
        try {
            ByteArrayInputStream bos = new ByteArrayInputStream(bytes);
            ObjectInputStream objectInputStream = new ObjectInputStream(bos);
            Object readObject = objectInputStream.readObject();
            objectInputStream.close();
            return readObject;
        } catch (Exception e) {
            throw new RuntimeException("Exception trying to deserialize bytes to obj, bytes="
                                               + new String(bytes),
                                       e);
        }

    }

}
