package io.s4.ft.wordcount;

import io.s4.ft.EventGenerator;
import io.s4.ft.KeyValue;
import io.s4.ft.S4TestCase;
import io.s4.ft.TestUtils;
import io.s4.wordcount.WordCountTest;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Test;

public class FTWordCountTest extends S4TestCase {

    private static Factory zookeeperServerConnectionFactory;

    @Test
    public void testSimple() throws Exception {
        TestUtils.cleanupTmpDirs();
        Process forkedS4App = null;
        try {
        // note: this should run automatically but does not...
        S4TestCase.initS4Parameters();
        forkedS4App = TestUtils.forkS4App(getClass().getName());

        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();
        final ZooKeeper zk = TestUtils.createZkClient();

        CountDownLatch signalTextProcessed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/textProcessed", signalTextProcessed,
                zk);
        EventGenerator gen = new EventGenerator();

        // add authorizations for processing
        for (int i = 1; i <= WordCountTest.SENTENCE_1_TOTAL_WORDS; i++) {
            System.out.println("$$$" + i);
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
        forkedS4App.destroy();
        
        forkedS4App = TestUtils.forkS4App(getClass().getName());
        gen.injectValueEvent(
                new KeyValue("sentence", WordCountTest.SENTENCE_2),
                "Sentences", 0);
        for (int i = WordCountTest.SENTENCE_1_TOTAL_WORDS + 1; i <= WordCountTest.SENTENCE_1_TOTAL_WORDS
                + WordCountTest.SENTENCE_2_TOTAL_WORDS; i++) {
            System.out.println("$$$" + i);
            zk.create("/continue_" + i, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
        }
        signalTextProcessed.await();
        File results = new File(System.getProperty("java.io.tmpdir")
                + File.separator + "wordcount");
        String s = TestUtils.readFile(results);
        Assert.assertEquals("be=2;da=2;doobie=4;not=1;or=1;to=2;", s);
        } finally {
            TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
            if (forkedS4App != null) {
                forkedS4App.destroy();
            }
        }

    }

    @After
    public void cleanup() throws IOException, InterruptedException {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);

    }

}
