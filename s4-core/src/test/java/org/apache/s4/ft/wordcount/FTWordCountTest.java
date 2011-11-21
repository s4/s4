package org.apache.s4.ft.wordcount;

import org.apache.s4.ft.EventGenerator;
import org.apache.s4.ft.KeyValue;
import org.apache.s4.ft.S4TestCase;
import org.apache.s4.ft.TestRedisStateStorage;
import org.apache.s4.ft.TestUtils;
import org.apache.s4.wordcount.WordCountTest;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * We use 2 lists of words that we inject in a word counting s4 system.
 * 
 * After processing the first sentence, we just kill the platform and restart
 * it.
 * 
 * Then we inject the second sentence.
 * 
 * 
 * We verify that no state was lost, i.e. that the words count includes words
 * from both the first and the second sentence.
 * 
 * NOTE 1: we synchronize through zookeeper to control when to kill the
 * application, and when to verify assertions. NOTE 2: we use some additional
 * explicit waits for bookkeeper backend so that it gets correctly initialized.
 * 
 * 
 */
public class FTWordCountTest extends S4TestCase {

    private static Factory zookeeperServerConnectionFactory;
    private static final String FILESYSTEM_BACKEND_CONF = "s4_core_conf_fs_backend.xml";
    private static final String REDIS_BACKEND_CONF = "s4_core_conf_redis_backend.xml";
    private Process forkedS4App = null;

    @Test
    public void testFileSystemBackend() throws Exception {
        doTestCheckpointingAndRecovery(FILESYSTEM_BACKEND_CONF);
    }

    @Test
    public void testRedisBackend() throws Exception {
        TestRedisStateStorage.runRedis();
        TestRedisStateStorage.clearRedis();
        doTestCheckpointingAndRecovery(REDIS_BACKEND_CONF);
        TestRedisStateStorage.stopRedis();
    }

    @Before
    public void prepare() throws Exception {
        TestUtils.cleanupTmpDirs();
        S4TestCase.initS4Parameters();
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();
    }

    @After
    public void cleanup() throws Exception {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        if (forkedS4App != null) {
            forkedS4App.destroy();
        }
    }

    // we send 1 sentence, wait for all words to be processed, then crash the
    // app
    // we do that for 3 sentences, in order to make sure that recovery does not
    // introduce side effects.
    public void doTestCheckpointingAndRecovery(String backendConf)
            throws Exception {
        final ZooKeeper zk = TestUtils.createZkClient();

        forkedS4App = TestUtils.forkS4App(getClass().getName(), backendConf);

        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/textProcessed", signalTextProcessed,
                zk);
        EventGenerator gen = new EventGenerator();

        // add authorizations for processing
        for (int i = 1; i <= WordCountTest.SENTENCE_1_TOTAL_WORDS; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }
        CountDownLatch signalSentence1Processed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/classifierIteration_"
                + WordCountTest.SENTENCE_1_TOTAL_WORDS,
                signalSentence1Processed, zk);
        gen.injectValueEvent(
                new KeyValue("sentence", WordCountTest.SENTENCE_1),
                "Sentences", 0);
        signalSentence1Processed.await(10, TimeUnit.SECONDS);
        Thread.sleep(1000);
        
        
        // crash the app
        forkedS4App.destroy();

        // recovering and making sure checkpointing still works
        forkedS4App = TestUtils.forkS4App(getClass().getName(), backendConf);

        // add authorizations for continuing processing. Without these, the
        // WordClassifier processed keeps waiting
        for (int i = WordCountTest.SENTENCE_1_TOTAL_WORDS + 1; i <= WordCountTest.SENTENCE_1_TOTAL_WORDS
                + WordCountTest.SENTENCE_2_TOTAL_WORDS; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }

        CountDownLatch sentence2Processed = new CountDownLatch(1);
        TestUtils
                .watchAndSignalCreation(
                        "/classifierIteration_"
                                + (WordCountTest.SENTENCE_1_TOTAL_WORDS + WordCountTest.SENTENCE_2_TOTAL_WORDS),
                        sentence2Processed, zk);

        gen.injectValueEvent(
                new KeyValue("sentence", WordCountTest.SENTENCE_2),
                "Sentences", 0);

        sentence2Processed.await(10, TimeUnit.SECONDS);
        Thread.sleep(1000);

        // crash the app
        forkedS4App.destroy();
        forkedS4App = TestUtils.forkS4App(getClass().getName(), backendConf);

        // add authorizations for continuing processing. Without these, the
        // WordClassifier processed keeps waiting
        for (int i = WordCountTest.SENTENCE_1_TOTAL_WORDS
                + WordCountTest.SENTENCE_2_TOTAL_WORDS + 1; i <= WordCountTest.TOTAL_WORDS; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }
        gen.injectValueEvent(
                new KeyValue("sentence", WordCountTest.SENTENCE_3),
                "Sentences", 0);
        signalTextProcessed.await(10, TimeUnit.SECONDS);
        File results = new File(S4TestCase.DEFAULT_TEST_OUTPUT_DIR
                + File.separator + "wordcount");
        String s = TestUtils.readFile(results);
        Assert.assertEquals("be=2;da=2;doobie=5;not=1;or=1;to=2;", s);

    }

}
