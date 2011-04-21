package io.s4.ft.wordcount;

import io.s4.ft.EventGenerator;
import io.s4.ft.KeyValue;
import io.s4.ft.S4TestCase;
import io.s4.ft.TestUtils;
import io.s4.wordcount.WordCountTest;

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
    private static final String BOOKKEEPER_BACKEND_CONF = "s4_core_conf_bk_backend.xml";
    private Process forkedS4App = null;

    @Test
    public void testFileSystemBackend() throws Exception {
        doTestCheckpointingAndRecovery(FILESYSTEM_BACKEND_CONF);
    }

    @Test
    public void bookKeeperBackend() throws Exception {
        doTestCheckpointingAndRecovery(BOOKKEEPER_BACKEND_CONF);
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

    public void doTestCheckpointingAndRecovery(String backendConf)
            throws Exception {
        final ZooKeeper zk = TestUtils.createZkClient();

        // note: this should run automatically but does not...
        if (BOOKKEEPER_BACKEND_CONF.equals(backendConf)) {
            TestUtils.initializeBKBookiesAndLedgers(zk);
        }

        forkedS4App = TestUtils.forkS4App(getClass().getName(), backendConf);

        if (BOOKKEEPER_BACKEND_CONF.equals(backendConf)) {
            // bookkeeper backend requires longer initialization
            // NOTE: we should rather find a way to use synchros...
            Thread.sleep(4000);
        }
        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/textProcessed", signalTextProcessed,
                zk);
        EventGenerator gen = new EventGenerator();

        // add authorizations for processing
        for (int i = 1; i <= WordCountTest.SENTENCE_1_TOTAL_WORDS; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }
        CountDownLatch signalSentence1Counted = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/classifierIteration_"
                + WordCountTest.SENTENCE_1_TOTAL_WORDS, signalSentence1Counted,
                zk);
        gen.injectValueEvent(
                new KeyValue("sentence", WordCountTest.SENTENCE_1),
                "Sentences", 0);
        signalSentence1Counted.await();
        // wait for asynchronous save operations to complete
        // NOTE: we should rather add synchros... 
        Thread.sleep(2000);
        if (BOOKKEEPER_BACKEND_CONF.equals(backendConf)) {
            // NOTE: we should rather add synchros...
            Thread.sleep(2000);
        }
        forkedS4App.destroy();

        forkedS4App = TestUtils.forkS4App(getClass().getName(), backendConf);
        if (BOOKKEEPER_BACKEND_CONF.equals(backendConf)) {
            // NOTE: we should rather add synchros...
            Thread.sleep(2000);
        }
        gen.injectValueEvent(
                new KeyValue("sentence", WordCountTest.SENTENCE_2),
                "Sentences", 0);

        // add authorizations for continuing processing. Without these, the
        // WordClassifier processed keeps waiting
        for (int i = WordCountTest.SENTENCE_1_TOTAL_WORDS + 1; i <= WordCountTest.SENTENCE_1_TOTAL_WORDS
                + WordCountTest.SENTENCE_2_TOTAL_WORDS; i++) {
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }
        signalTextProcessed.await(20, TimeUnit.SECONDS);
        File results = new File(System.getProperty("java.io.tmpdir")
                + File.separator + "wordcount");
        String s = TestUtils.readFile(results);
        Assert.assertEquals("be=2;da=2;doobie=4;not=1;or=1;to=2;", s);
    }

}
