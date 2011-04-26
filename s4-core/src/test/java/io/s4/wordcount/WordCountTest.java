package io.s4.wordcount;

import io.s4.ft.EventGenerator;
import io.s4.ft.KeyValue;
import io.s4.ft.S4TestCase;
import io.s4.ft.TestUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class WordCountTest extends S4TestCase {
    public static final String SENTENCE_1 = "to be or not to be doobie doobie da";
    public static final int SENTENCE_1_TOTAL_WORDS = SENTENCE_1.split(" ").length;
    public static final String SENTENCE_2 = "doobie doobie da";
    public static final int SENTENCE_2_TOTAL_WORDS = SENTENCE_2.split(" ").length;
    public static final String FLAG = ";";
    public static final int TOTAL_WORDS = SENTENCE_1_TOTAL_WORDS
            + SENTENCE_2_TOTAL_WORDS;
    private static Factory zookeeperServerConnectionFactory;

    @BeforeClass
    public static void cleanupTmpDirs() {
        // FIXME why isn't this called automatically???
        TestUtils.cleanupTmpDirs();
    }

    @Test
    public void testSimple() throws Exception {
        TestUtils.cleanupTmpDirs();
        // note: this should run automatically but does not...
        S4TestCase.initS4Parameters();
        initializeS4App(getClass(), "s4_core_conf.xml");
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();
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
        signalTextProcessed.await();
        File results = new File(S4TestCase.DEFAULT_TEST_OUTPUT_DIR
                + File.separator + "wordcount");
        String s = TestUtils.readFile(results);
        Assert.assertEquals("be=2;da=2;doobie=4;not=1;or=1;to=2;", s);
        
    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);

    }

}
