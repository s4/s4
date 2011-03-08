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
package io.s4.comm.zk;

import io.s4.comm.core.DefaultWatcher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

public class ZkUtil extends DefaultWatcher {

    public ZkUtil(String address) {
        super(address);

    }

    public int getChildCount(String path) {
        try {
            List<String> children = zk.getChildren(path, false);
            return children.size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getChildren(String path) {
        try {
            List<String> children = zk.getChildren(path, false);
            return children;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] getData(String path) {
        try {
            byte[] data = zk.getData(path, false, null);
            return data;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void create(String path) {
        create(path, "");
    }

    public void create(String path, String data) {
        try {
            zk.create(path,
                      data.getBytes(),
                      Ids.OPEN_ACL_UNSAFE,
                      CreateMode.PERSISTENT);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void deleteRecursive(String path) {
        List<String> children = getChildren(path);
        for (String child : children) {
            deleteRecursive(path + "/" + child);
        }
        delete(path);
    }

    public void delete(String path) {
        try {
            zk.delete(path, -1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();

        }
        String address = args[0];
        String methodName = args[1];

        String[] methodArgs = new String[args.length - 2];
        for (int i = 2; i < args.length; i++) {
            methodArgs[i - 2] = args[i];
        }
        Method[] methods = ZkUtil.class.getMethods();
        Method method = null;
        for (Method met : methods) {
            if (met.getName().equals(methodName)
                    && met.getParameterTypes().length == methodArgs.length) {
                method = met;
                break;
            }
        }

        if (method != null) {
            ZkUtil zkUtil = new ZkUtil(address);
            Object ret = method.invoke(zkUtil, methodArgs);
            if (ret != null) {
                System.out.println("**********");
                System.out.println(ret);
                System.out.println("**********");
            }
        } else {
            printUsage();
        }
        // zkUtil.deleteRecursive("/s4/listener/process/task-0");
        // zkUtil.create("/s4_apps_test/sender/process");
    }

    private static void printUsage() {
        System.out.println("USAGE");
        System.out.println("java <zkadress> methodName arguments");
        Method[] methods = ZkUtil.class.getMethods();
        for (Method met : methods) {
            System.out.println(met.getName() + ":"
                    + Arrays.toString(met.getParameterTypes()));
        }
        System.exit(1);
    }
}
