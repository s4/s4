package io.s4.wordcount;

import io.s4.ft.EventGenerator;
import io.s4.ft.KeyValue;
import io.s4.ft.S4App;
import io.s4.ft.S4TestCase;
import io.s4.ft.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class WordCountTest extends S4TestCase {
    public static final String SENTENCE_1 = "to be or not to be doobie doobie da";
    public static final int SENTENCE_1_TOTAL_WORDS = SENTENCE_1.split(" ").length;
    public static final String SENTENCE_2 = "doobie doobie da";
    public static final int SENTENCE_2_TOTAL_WORDS = SENTENCE_2.split(" ").length;
    public static final String SENTENCE_3 = "doobie";
    public static final int SENTENCE_3_TOTAL_WORDS = SENTENCE_3.split(" ").length;
    public static final String FLAG = ";";
    public static int TOTAL_WORDS = SENTENCE_1_TOTAL_WORDS
            + SENTENCE_2_TOTAL_WORDS + SENTENCE_3_TOTAL_WORDS;
    private static Factory zookeeperServerConnectionFactory;

    
    @Before
    public void prepare() throws IOException, InterruptedException, KeeperException {
        TestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();

    }

    @Test
    public void testSimple() throws Exception {
        
        S4App app = new S4App(getClass(), "s4_core_conf.xml");
        app.initializeS4App();
        final ZooKeeper zk = TestUtils.createZkClient();

        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/textProcessed", signalTextProcessed,
                zk);
        EventGenerator gen = new EventGenerator();
        
        // add authorizations for processing
        for (int i = 1; i <= SENTENCE_1_TOTAL_WORDS + SENTENCE_2_TOTAL_WORDS
                + 1; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }
        gen.injectValueEvent(new KeyValue("sentence", SENTENCE_1),
                    "Sentences", 0);
        gen.injectValueEvent(new KeyValue("sentence", SENTENCE_2), "Sentences",
                0);
        gen.injectValueEvent(new KeyValue("sentence", SENTENCE_3), "Sentences",
                0);
        signalTextProcessed.await();
        File results = new File(S4TestCase.DEFAULT_TEST_OUTPUT_DIR
                + File.separator + "wordcount");
        String s = TestUtils.readFile(results);
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", s);
        
    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);

    }

}
