package io.s4.ft;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

public class TestUtils {

    public static Process forkS4App(String testClassName) throws IOException,
            InterruptedException {
        TestUtils.killS4App(testClassName);
        List<String> cmdList = new ArrayList<String>();
        cmdList.add("java");
        cmdList.add("-cp");
        cmdList.add(System.getProperty("java.class.path"));
        cmdList.add("-Dcommlayer_mode=static");
        cmdList.add("-Dcommlayer.mode=static");
        cmdList.add("-Dlock_dir=" + S4TestCase.lockDirPath);
        cmdList.add("-Dlog4j.configuration=file:///Users/matthieu/log4j.xml");
        cmdList.add("-Xdebug");
        cmdList.add("-Xnoagent");
        cmdList.add("-Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n");
        cmdList.add(S4App.class.getName());
        cmdList.add(testClassName);
    
        ProcessBuilder pb = new ProcessBuilder(cmdList);
        pb.directory(new File(System.getProperty("user.dir")));
        pb.redirectErrorStream();
        pb.toString();
        final Process process = pb.start();
    
        // if (start.exitValue() != 0) {
        // System.out.println("here");
        // }
        new Thread(new Runnable() {
            public void run() {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        process.getInputStream()));
                String line;
                try {
                    line = br.readLine();
                    while (line != null) {
                        System.out.println(line);
                        line = br.readLine();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    
        return process;
    }

    public static void killS4App(String testClassName) throws IOException,
            InterruptedException {
    
        List<String> cmdList = new ArrayList<String>();
        cmdList.add(System.getProperty("user.dir")
                + "/src/test/scripts/killJavaProcessForPort.sh");
        cmdList.add("*:50770");
        ProcessBuilder pb = new ProcessBuilder(cmdList);
        pb.redirectErrorStream();
        // pb.directory(new File(System.getProperty("user.dir")));
        final Process start = pb.start();
        new Thread(new Runnable() {
            public void run() {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        start.getInputStream()));
                String line;
                try {
                    line = br.readLine();
                    while (line != null) {
                        System.out.println(line);
                        line = br.readLine();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
    
        start.waitFor();
    
    }

    public static void writeStringToFile(String s, File f) throws IOException {
        if (f.exists()) {
            if (!f.delete()) {
                throw new RuntimeException("Cannot delete file "
                        + f.getAbsolutePath());
            }
        }
    
        FileWriter fw = null;
        try {
            if (!f.createNewFile()) {
                throw new RuntimeException("Cannot create new file : "
                        + f.getAbsolutePath());
            }
            fw = new FileWriter(f);
    
            fw.write(s);
        } catch (IOException e) {
            throw (e);
        } finally {
            if (fw != null) {
                try {
                    fw.close();
                } catch (IOException e) {
                    throw (e);
                }
            }
        }
    }

    public static String readFile(File f) throws IOException {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(f));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
                if (line != null) {
                    sb.append("\n");
                }
            }
            return sb.toString();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    throw (e);
                }
            }
        }
    
    }

    public static void startZookeeperServer() throws IOException,
            InterruptedException {
        TestUtils.stopZookeeperServer();
        List<String> cmdList = new ArrayList<String>();
        File zkDataDir = new File(System.getProperty("user.dir")
                + File.separator + "tmp" + File.separator + "zookeeper"
                + File.separator + "data");
        if (zkDataDir.exists()) {
            TestUtils.deleteDirectoryContents(zkDataDir);
        } else {
            zkDataDir.mkdirs();
        }
    
        cmdList.add(System.getProperty("user.dir") + File.separator + "src"
                + File.separator + "test" + File.separator + "scripts"
                + File.separator + "zooKeeperServer.sh");
        cmdList.add(System.getProperty("java.class.path"));
        cmdList.add("21810");
        cmdList.add(zkDataDir.getAbsolutePath());
        ProcessBuilder builder = new ProcessBuilder(cmdList);
        builder.directory(new File(System.getProperty("user.dir")));
        builder.redirectErrorStream(true);
        final Process start = builder.start();
        new Thread(new Runnable() {
            public void run() {
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        start.getInputStream()));
                String line;
                try {
                    line = br.readLine();
                    while (line != null) {
                        System.out.println(line);
                        line = br.readLine();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
        // new File(USER_DIR + "/tmp/zookeeper/data").mkdirs();
        // //
    
    }

    public static void stopZookeeperServer() throws IOException,
            InterruptedException {
        List<String> cmdList = new ArrayList<String>();
        cmdList.add(System.getProperty("user.dir")
                + "/src/test/scripts/killJavaProcessForPort.sh");
        cmdList.add("*:21810");
        // int zkPid = Integer.valueOf(readFileAsString(new File(System
        // .getProperty("user.dir")
        // + File.separator
        // + "tmp"
        // + File.separator + "zk.pid")));
        // cmdList.add(String.valueOf(zkPid));
        ProcessBuilder pb = new ProcessBuilder(cmdList);
        // pb.directory(new File(System.getProperty("user.dir")));
        pb.start().waitFor();
    
    }

    public static void deleteDirectoryContents(File dir) {
        File[] files = dir.listFiles();
        for (File file : files) {
            if (file.isDirectory()) {
                deleteDirectoryContents(file);
            }
            if (!file.delete()) {
                throw new RuntimeException("could not delete : " + file);
            }
        }
    }

    public static String readFileAsString(File f) throws IOException {
        FileReader fr = new FileReader(f);
        StringBuilder sb = new StringBuilder("");
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        while (line != null) {
            sb.append(line);
            line = br.readLine();
            if (line != null) {
                sb.append("\n");
            }
        }
        return sb.toString();
    
    }

    // TODO factor this code (see BasicFSStateStorage) - or use commons io or
    // guava
    public static byte[] readFileAsByteArray(File file) throws Exception {
        FileInputStream in = null;
        try {
            in = new FileInputStream(file);
    
            long length = file.length();
    
            /*
             * Arrays can only be created using int types, so ensure that the
             * file size is not too big before we downcast to create the array.
             */
            if (length > Integer.MAX_VALUE) {
                throw new IOException("Error file is too large: "
                        + file.getName() + " " + length + " bytes");
            }
    
            byte[] buffer = new byte[(int) length];
            int offSet = 0;
            int numRead = 0;
    
            while (offSet < buffer.length
                    && (numRead = in.read(buffer, offSet, buffer.length
                            - offSet)) >= 0) {
                offSet += numRead;
            }
    
            if (offSet < buffer.length) {
                throw new IOException("Error, could not read entire file: "
                        + file.getName() + " " + offSet + "/" + buffer.length
                        + " bytes read");
            }
    
            in.close();
            return buffer;
    
        } finally {
            if (in != null) {
                    in.close();
            }
        }
    }

    public static ZooKeeper createZkClient() throws IOException {
        final ZooKeeper zk = new ZooKeeper("localhost:21810", 4000,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                    }
                });
        return zk;
    }

    public static void watchAndSignalCreation(String path,
            final CountDownLatch latch, final ZooKeeper zk)
            throws KeeperException, InterruptedException {
    
        if (zk.exists(path, false) != null) {
            zk.delete(path, -1);
        }
        zk.exists(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (EventType.NodeCreated.equals(event.getType())) {
                    latch.countDown();
                }
            }
        });
    }

}
