package io.s4.ft;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;


import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

public class TestBKStateStorage extends TestCase {
    static final Logger LOG = Logger.getLogger(TestBKStateStorage.class);
    
    private static final String PAYLOAD = "payload";
    private BookKeeperStateStorage bkstore; 
    // ZooKeeper related variables
    static final String HOSTPORT = "127.0.0.1:2181";
    static Integer ZooKeeperDefaultPort = 2181;
    ZooKeeperServer zks;
    ZooKeeper zkc; // zookeeper client
    NIOServerCnxnFactory serverFactory;
    File ZkTmpDir;

    // BookKeeper
    List<File> tmpDirs = new ArrayList<File>();
    List<BookieServer> bs = new ArrayList<BookieServer>();
    Integer initialPort = 5000;
    int numBookies = 3;
    BookKeeper bkc;
    
    @Parameters
    public static Collection<Object[]> configs(){
        return Arrays.asList(new Object[][]{ {DigestType.MAC }, {DigestType.CRC32}});
    }


    @Before
    @Override
    public void setUp() throws Exception {
        try {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        LOG.debug("Running ZK server");
        // ServerStats.registerAsConcrete();
        ClientBase.setupTestEnv();
        ZkTmpDir = File.createTempFile("zookeeper", "test");
        ZkTmpDir.delete();
        ZkTmpDir.mkdir();

        zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperDefaultPort);
        serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(new InetSocketAddress(ZooKeeperDefaultPort), 100);
        serverFactory.startup(zks);

        boolean b = ClientBase.waitForServerUp(HOSTPORT, ClientBase.CONNECTION_TIMEOUT);

        LOG.debug("Server up: " + b);

        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        zkc = new ZooKeeper("127.0.0.1", ZooKeeperDefaultPort, new emptyWatcher());

        // initialize the zk client with values
        zkc.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        // Create Bookie Servers (B1, B2, B3)
        for (int i = 0; i < numBookies; i++) {
            File f = File.createTempFile("bookie", "test");
            tmpDirs.add(f);
            f.delete();
            f.mkdir();

            BookieServer server = new BookieServer(initialPort + i, HOSTPORT, f, new File[] { f });
            server.start();
            bs.add(server);
        }
        zkc.close();
        bkc = new BookKeeper("127.0.0.1");
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        }
        
        this.bkstore = new BookKeeperStateStorage(HOSTPORT);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        LOG.info("TearDown");

        if (bkc != null) {
            bkc.halt();;
        }
        
        for (BookieServer server : bs) {
            server.shutdown();
        }

        for (File f : tmpDirs) {
            cleanUpDir(f);
        }

        // shutdown ZK server
        if (serverFactory != null) {
            serverFactory.shutdown();
            assertTrue("waiting for server down", ClientBase.waitForServerDown(HOSTPORT, ClientBase.CONNECTION_TIMEOUT));
        }
        // ServerStats.unregister();
        cleanUpDir(ZkTmpDir);
        

    }

    /* Clean up a directory recursively */
    protected boolean cleanUpDir(File dir) {
        if (dir.isDirectory()) {
            LOG.info("Cleaning up " + dir.getName());
            String[] children = dir.list();
            for (String string : children) {
                boolean success = cleanUpDir(new File(dir, string));
                if (!success)
                    return false;
            }
        }
        // The directory is now empty so delete it
        return dir.delete();
    }

    /* User for testing purposes, void */
    class emptyWatcher implements Watcher {
        public void process(WatchedEvent event) {
        }
    }
    
    @Test
    public void testFetchState() throws IOException, InterruptedException {
        SafeKeeperId key = new SafeKeeperId("stream", "prototype", "classname", "key");
        bkstore.saveState(key, PAYLOAD.getBytes(), null);
        byte[] result = bkstore.fetchState(key);
        String recovered = new String(result);
        assertEquals(PAYLOAD, recovered);
    }

    @Test
    public void testFetchStoredKeys() {
        Set<SafeKeeperId> fixture = new HashSet<SafeKeeperId>();
        for (int i = 0; i < 10; i++)
            fixture.add(new SafeKeeperId("stream", "prototype", "classname", "key" + i));
        for (SafeKeeperId skid : fixture)
            bkstore.saveState(skid, PAYLOAD.getBytes(), null);

        // retrieve the keys
        Set<SafeKeeperId> result = bkstore.fetchStoredKeys();
        assertEquals(fixture.size(), result.size());
        assertEquals(fixture, result);
    }
    
}
