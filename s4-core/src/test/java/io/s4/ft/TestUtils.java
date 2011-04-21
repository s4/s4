package io.s4.ft;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;

public class TestUtils {

    public static final int ZK_PORT = 21810;
    public static final int INITIAL_BOOKIE_PORT = 5000;
    static List<BookieServer> bs = new ArrayList<BookieServer>();
    public static Process forkS4App(String testClassName, String s4CoreConfFileName) throws IOException,
            InterruptedException {
        List<String> cmdList = new ArrayList<String>();
        cmdList.add("java");
        cmdList.add("-cp");
        cmdList.add(System.getProperty("java.class.path"));
        cmdList.add("-Dcommlayer_mode=static");
        cmdList.add("-Dcommlayer.mode=static");
        cmdList.add("-Dlock_dir=" + S4TestCase.lockDirPath);
        cmdList.add("-Dlog4j.configuration=file://"
                + System.getProperty("user.dir")
                + "/src/test/resources/log4j.xml");
//        cmdList.add("-Xdebug");
//        cmdList.add("-Xnoagent");
//        cmdList.add("-Xrunjdwp:transport=dt_socket,address=8788,server=y,suspend=n");
        cmdList.add(S4App.class.getName());
        cmdList.add(testClassName);
        cmdList.add(s4CoreConfFileName);

        ProcessBuilder pb = new ProcessBuilder(cmdList);
        pb.directory(new File(System.getProperty("user.dir")));
        pb.redirectErrorStream();
        pb.toString();
        final Process process = pb.start();
        // TODO some synchro with s4 platform ready state
        Thread.sleep(1500);

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

    public static void killS4App(Process forkedApp) throws IOException,
            InterruptedException {
        if (forkedApp != null) {
            forkedApp.destroy();
        }
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

    public static NIOServerCnxn.Factory startZookeeperServer()
            throws IOException, InterruptedException, KeeperException {

        List<String> cmdList = new ArrayList<String>();
        final File zkDataDir = new File(System.getProperty("user.dir")
                + File.separator + "tmp" + File.separator + "zookeeper"
                + File.separator + "data");
        if (zkDataDir.exists()) {
            TestUtils.deleteDirectoryContents(zkDataDir);
        } else {
            zkDataDir.mkdirs();
        }

        ZooKeeperServer zks = new ZooKeeperServer(zkDataDir, zkDataDir, 3000);
        // SyncRequestProcessor.setSnapCount(1000);
        // final int PORT = Integer.parseInt(HOSTPORT.split(":")[1]);
        NIOServerCnxn.Factory nioZookeeperConnectionFactory = new NIOServerCnxn.Factory(
                new InetSocketAddress(ZK_PORT));
        nioZookeeperConnectionFactory.startup(zks);
        Assert.assertTrue("waiting for server being up",
                waitForServerUp("localhost", ZK_PORT, 4000));
        return nioZookeeperConnectionFactory;

    }

    public static void stopZookeeperServer(NIOServerCnxn.Factory f)
            throws IOException, InterruptedException {
        if (f != null) {
            f.shutdown();
            Assert.assertTrue("waiting for server down",
                    waitForServerDown("localhost", ZK_PORT, 3000));
        }
        // List<String> cmdList = new ArrayList<String>();
        // cmdList.add(System.getProperty("user.dir")
        // + "/src/test/scripts/killJavaProcessForPort.sh");
        // cmdList.add("*:21810");
        // // int zkPid = Integer.valueOf(readFileAsString(new File(System
        // // .getProperty("user.dir")
        // // + File.separator
        // // + "tmp"
        // // + File.separator + "zk.pid")));
        // // cmdList.add(String.valueOf(zkPid));
        // ProcessBuilder pb = new ProcessBuilder(cmdList);
        // // pb.directory(new File(System.getProperty("user.dir")));
        // pb.start().waitFor();

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
        final ZooKeeper zk = new ZooKeeper("localhost:" + ZK_PORT, 4000,
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
    
    public static void watchAndSignalChangedChildren(String path,
            final CountDownLatch latch, final ZooKeeper zk)
            throws KeeperException, InterruptedException {

        zk.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (EventType.NodeChildrenChanged.equals(event.getType())) {
                    latch.countDown();
                }
            }
        });
    }

    // from zookeeper's codebase
    public static boolean waitForServerUp(String host, int port, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                // if there are multiple hostports, just take the first one
                String result = send4LetterWord(host, port, "stat");
                if (result.startsWith("Zookeeper version:")) {
                    return true;
                }
            } catch (IOException ignored) {
                // ignore as this is expected
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

    // from zookeeper's codebase
    public static String send4LetterWord(String host, int port, String cmd)
            throws IOException {
        Socket sock = new Socket(host, port);
        BufferedReader reader = null;
        try {
            OutputStream outstream = sock.getOutputStream();
            outstream.write(cmd.getBytes());
            outstream.flush();
            // this replicates NC - close the output stream before reading
            sock.shutdownOutput();

            reader = new BufferedReader(new InputStreamReader(
                    sock.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            return sb.toString();
        } finally {
            sock.close();
            if (reader != null) {
                reader.close();
            }
        }
    }

    // from zookeeper's codebase
    public static boolean waitForServerDown(String host, int port, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            try {
                send4LetterWord(host, port, "stat");
            } catch (IOException e) {
                return true;
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

    public static void cleanupTmpDirs() {
        if (S4TestCase.DEFAULT_TEST_OUTPUT_DIR.exists()) {
            deleteDirectoryContents(S4TestCase.DEFAULT_TEST_OUTPUT_DIR);
        }
        S4TestCase.DEFAULT_STORAGE_DIR.mkdirs();
    
    }

    public static void initializeBKBookiesAndLedgers(final ZooKeeper zk)
            throws KeeperException, InterruptedException, IOException {
        try {
            zk.delete("/ledgers/available", -1);
        } catch (Exception ignored) {
        }
    
        try {
            zk.delete("/ledgers", -1);
        } catch (Exception ignored) {
        }
        
        try {
            zk.delete("/s4/checkpoints", -1);
        } catch (Exception ignored) {
        }
    
        try {
            zk.delete("/s4", -1);
        } catch (Exception ignored) {
        }
        
        zk.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk.create("/ledgers/available", new byte[0],
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create("/s4", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        zk.create("/s4/checkpoints", new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
     // Create Bookie Servers (B1, B2, B3)
        for (int i = 0; i < 3; i++) {
            File f = new File(S4TestCase.DEFAULT_STORAGE_DIR+"/bookie_test_" +i);
            f.delete();
            f.mkdir();

            BookieServer server = new BookieServer(INITIAL_BOOKIE_PORT + i, "localhost:"+ZK_PORT, f, new File[] { f });
            server.start();
            bs.add(server);
        }
//        this.bkstore = new BookKeeperStateStorage("localhost:"+ZK_PORT);

    }
    
    public static void stopBKBookies() throws Exception {
        if (bs!=null) {
            for (BookieServer bookie : bs) {
                bookie.shutdown();
            }
        }
    }

}
