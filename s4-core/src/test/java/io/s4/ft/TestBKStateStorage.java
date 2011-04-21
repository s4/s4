package io.s4.ft;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class TestBKStateStorage {

    static final Logger LOG = Logger.getLogger(TestBKStateStorage.class);

    private static final String PAYLOAD = "payload";
    private BookKeeperStateStorage bkstore;

    // BookKeeper
    List<File> tmpDirs = new ArrayList<File>();
    List<BookieServer> bs = new ArrayList<BookieServer>();
    int numBookies = 3;
    BookKeeper bkc;

    private static Factory zkServerFactory;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { DigestType.MAC },
                { DigestType.CRC32 } });
    }

    @Before
    public void prepare() throws Throwable {
        TestUtils.cleanupTmpDirs();
        zkServerFactory = TestUtils.startZookeeperServer();
        final ZooKeeper zk = TestUtils.createZkClient();
        TestUtils.initializeBKBookiesAndLedgers(zk);
        

        bkstore = new BookKeeperStateStorage();
        bkstore.setZkServers("localhost:"
                +
                String.valueOf(TestUtils.ZK_PORT));
        bkstore.setEnsembleSize(1);
        bkstore.setQuorumSize(1);
        bkstore.init();
        Thread.sleep(2000);
        zk.close();
    }

    @After
    public void cleanup() throws Exception {
        TestUtils.stopBKBookies();
        TestUtils.stopZookeeperServer(zkServerFactory);
        TestUtils.cleanupTmpDirs();

    }

    /* User for testing purposes, void */
    class emptyWatcher implements Watcher {
        public void process(WatchedEvent event) {
        }
    }

    @Test
    public void testFetchState() throws IOException, InterruptedException {
        SafeKeeperId key = new SafeKeeperId("prototype", "classname", "key");
        bkstore.saveState(key, PAYLOAD.getBytes(), null);
        Thread.sleep(1000);
        byte[] result = bkstore.fetchState(key);
        String recovered = new String(result);
        assertEquals(PAYLOAD, recovered);
    }

    @Test
    public void testFetchStoredKeys() throws InterruptedException {
        Set<SafeKeeperId> fixture = new HashSet<SafeKeeperId>();
        for (int i = 0; i < 10; i++) {
            fixture.add(new SafeKeeperId("prototype", "classname", "key" + i));
        }
        for (SafeKeeperId skid : fixture) {
            bkstore.saveState(skid, PAYLOAD.getBytes(), null);
        }
        Thread.sleep(4000);

        // retrieve the keys
        Set<SafeKeeperId> result = bkstore.fetchStoredKeys();
        assertEquals(fixture.size(), result.size());
        assertEquals(fixture, result);
    }

}
