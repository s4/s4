package org.apache.s4.ft;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryTest extends S4TestCase {

    public static long ZOOKEEPER_PORT = 21810;
    private Process forkedS4App = null;
    private static Factory zookeeperServerConnectionFactory = null;

    @Before
    public void prepare() throws Exception {
        TestUtils.cleanupTmpDirs();
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();
        final ZooKeeper zk = TestUtils.createZkClient();
        try {
            zk.delete("/value1Set", -1);
        } catch (Exception ignored) {
        }
        try {
            // FIXME can't figure out where this is retained
            zk.delete("/value2Set", -1);
        } catch (Exception ignored) {
        }
        try {
            // FIXME can't figure out where this is retained
            zk.delete("/checkpointed", -1);
        } catch (Exception ignored) {
        }
        zk.close();
    }

    @After
    public void cleanup() throws Exception {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        TestUtils.killS4App(forkedS4App);
    }

    @Test
    public void testCheckpointRestorationThroughApplicationEvent()
            throws Exception {
        final ZooKeeper zk = TestUtils.createZkClient();
        // 1. instantiate remote S4 app
        forkedS4App = TestUtils.forkS4App(getClass().getName(),
                "s4_core_conf_fs_backend.xml");
        // TODO synchro
        Thread.sleep(4000);

        CountDownLatch signalValue1Set = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/value1Set", signalValue1Set, zk);

        // 2. generate a simple event that changes the state of the PE
        // --> this event triggers recovery
        // we inject a value for value2 field (was for value1 in
        // checkpointing
        // test). This should trigger recovery and provide a pe with value1
        // and
        // value2 set:
        // value1 from recovery, and value2 from injected event.
        EventGenerator gen = new EventGenerator();
        gen.injectValueEvent(new KeyValue("value1", "message1"), "Stream1", 0);
        signalValue1Set.await();
        final CountDownLatch signalCheckpointed = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/checkpointed", signalCheckpointed,
                zk);
        // trigger checkpoint
        gen.injectValueEvent(new KeyValue("initiateCheckpoint", "blah"),
                "Stream1", 0);
        signalCheckpointed.await();
        // signalCheckpointAddedByBK.await();

        signalValue1Set = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/value1Set", signalValue1Set, zk);
        gen.injectValueEvent(new KeyValue("value1", "message1b"), "Stream1", 0);
        signalValue1Set.await();
        Assert.assertEquals("value1=message1b ; value2=",
                TestUtils.readFile(StatefulTestPE.DATA_FILE));

        Thread.sleep(2000);
        // kill app
        forkedS4App.destroy();
        // S4App.killS4App(getClass().getName());

        StatefulTestPE.DATA_FILE.delete();

        forkedS4App = TestUtils.forkS4App(getClass().getName(),
                "s4_core_conf_fs_backend.xml");
        // TODO synchro
        Thread.sleep(2000);
        // trigger recovery by sending application event to set value 2
        CountDownLatch signalValue2Set = new CountDownLatch(1);
        TestUtils.watchAndSignalCreation("/value2Set", signalValue2Set, zk);

        gen.injectValueEvent(new KeyValue("value2", "message2"), "Stream1", 0);
        signalValue2Set.await(10, TimeUnit.SECONDS);

        // we should get "message1" (checkpointed) instead of "message1b"
        // (latest)
        Assert.assertEquals("value1=message1 ; value2=message2",
                TestUtils.readFile(StatefulTestPE.DATA_FILE));

    }

}
