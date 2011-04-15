package io.s4.ft;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeperMain;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.Test;

public class RecoveryTest extends S4TestCase {

    // TODO parameter
    public static long ZOOKEEPER_PORT = 21810;


    @Test
    public void testCheckpointRestorationThroughApplicationEvent()
            throws Exception {
        Factory zookeeperServerConnectionFactory = null;
        Process forkedS4App = null;
        try {
            // note: this should run automatically but does not...
            S4TestCase.initS4Parameters();

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

            // 0. cleanup storage dir

            // cleanup

            // 1. instantiate remote S4 app
            forkedS4App = TestUtils.forkS4App(getClass().getName());
            // TODO synchro
            Thread.sleep(2000);

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
            gen.injectValueEvent(new KeyValue("value1", "message1"), "Stream1",
                    0);
            signalValue1Set.await();
            final CountDownLatch signalCheckpointed = new CountDownLatch(1);
            TestUtils.watchAndSignalCreation("/checkpointed",
                    signalCheckpointed, zk);
            // trigger checkpoint
            gen.injectValueEvent(new KeyValue("initiateCheckpoint", "blah"),
                    "Stream1", 0);
            signalCheckpointed.await();

            signalValue1Set = new CountDownLatch(1);
            TestUtils.watchAndSignalCreation("/value1Set", signalValue1Set, zk);
            gen.injectValueEvent(new KeyValue("value1", "message1b"),
                    "Stream1", 0);
            signalValue1Set.await();
            Assert.assertEquals("value1=message1b ; value2=",
                    TestUtils.readFile(StatefulTestPE.DATA_FILE));
            // kill app
            forkedS4App.destroy();
            // S4App.killS4App(getClass().getName());

            StatefulTestPE.DATA_FILE.delete();

            forkedS4App = TestUtils.forkS4App(getClass().getName());
            // TODO synchro
            Thread.sleep(2000);
            // trigger recovery by sending application event to set value 2
            CountDownLatch signalValue2Set = new CountDownLatch(1);
            TestUtils.watchAndSignalCreation("/value2Set", signalValue2Set, zk);

            gen.injectValueEvent(new KeyValue("value2", "message2"), "Stream1",
                    0);
            signalValue2Set.await();

            System.out.println("#2");
            // we should get "message1" (checkpointed) instead of "message1b"
            // (latest)
            Assert.assertEquals("value1=message1 ; value2=message2",
                    TestUtils.readFile(StatefulTestPE.DATA_FILE));
        }

        finally {
            TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
            TestUtils.killS4App(forkedS4App);
        }
    }

    // @Test
    public void testCheckpointRestorationThroughEagerFetching() {
        // 1. instantiate a checkpointable PE

        // 2. generate a simple event that changes the state of the PE

        // 3. generate a checkpoint event

        // 4. restart

        // 5. verify that, after some time (for instance), PE instance is
        // available and restored to a correct state
    }

}
