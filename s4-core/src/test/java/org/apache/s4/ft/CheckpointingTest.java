package org.apache.s4.ft;

import org.apache.s4.serialize.KryoSerDeser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Base64;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.reflectasm.FieldAccess;

public class CheckpointingTest extends S4TestCase {

    private static Factory zookeeperServerConnectionFactory = null;
    private S4App app;

    @Before
    public void prepare() throws Exception {
        zookeeperServerConnectionFactory = TestUtils.startZookeeperServer();
        app = new S4App(getClass(), "s4_core_conf_fs_backend.xml");
        app.initializeS4App();
    }

    @After
    public void cleanup() throws Exception {
        TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        if (app!=null) {
            app.destroy();
        }
    }

    @Test
    public void testCheckpointStorage() throws Exception {
            final ZooKeeper zk = TestUtils.createZkClient();

            

            // 2. generate a simple event that creates and changes the state of
            // the
            // PE

            // NOTE: coordinate through zookeeper
            final CountDownLatch signalValue1Set = new CountDownLatch(1);

            TestUtils.watchAndSignalCreation("/value1Set", signalValue1Set, zk);
            final CountDownLatch signalCheckpointed = new CountDownLatch(1);
            TestUtils.watchAndSignalCreation("/checkpointed",
                    signalCheckpointed, zk);
            EventGenerator gen = new EventGenerator();
            gen.injectValueEvent(new KeyValue("value1", "message1"), "Stream1",
                    0);

            signalValue1Set.await();
            StatefulTestPE pe = (StatefulTestPE) S4TestCase.registeredPEs
                    .get(new SafeKeeperId("statefulPE", "value"));
            Assert.assertEquals("message1", pe.getValue1());
            Assert.assertEquals("", pe.getValue2());

            // 3. generate a checkpoint event 
            gen.injectValueEvent(new KeyValue("initiateCheckpoint", "blah"),
                    "Stream1", 0);
            signalCheckpointed.await();

            // NOTE: the backend has asynchronous save operations
            Thread.sleep(1000);

            SafeKeeperId safeKeeperId = pe.getSafeKeeperId();
            File expected = new File(System.getProperty("user.dir")
                    + File.separator
                    + "tmp"
                    + File.separator
                    + "storage"
                    + File.separator
                    + safeKeeperId.getPrototypeId()
                    + File.separator
                    + Base64.encodeBase64URLSafeString(safeKeeperId
                            .getStringRepresentation().getBytes()));

            // 4. verify that state was correctly persisted
            Assert.assertTrue(expected.exists());

            StatefulTestPE refPE = new StatefulTestPE();
            refPE.setValue1("message1");
            refPE.setId("statefulPE");
            refPE.setKeys(new String[] {});
            KryoSerDeser kryoSerDeser = new KryoSerDeser();
            byte[] refBytes = kryoSerDeser.serialize(refPE);

            Assert.assertTrue(Arrays.equals(refBytes,
                    TestUtils.readFileAsByteArray(expected)));

            app.destroy();
            
    }

}
