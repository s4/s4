package io.s4.ft;

import io.s4.serialize.KryoSerDeser;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Base64;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.Test;

import com.esotericsoftware.reflectasm.FieldAccess;

public class CheckpointingTest extends S4TestCase {

    private static boolean delete;
    public static File DEFAULT_TEST_OUTPUT_DIR = new File(
            System.getProperty("user.dir") + File.separator + "tmp");
    public static File DEFAULT_STORAGE_DIR = new File(
            DEFAULT_TEST_OUTPUT_DIR.getAbsolutePath() + File.separator
                    + "storage");

    // TODO add timeout
    @Test
    public void testCheckpointStorage() throws Exception {
        Factory zookeeperServerConnectionFactory = null;
        try {

            zookeeperServerConnectionFactory = TestUtils
                    .startZookeeperServer();
            final ZooKeeper zk = TestUtils.createZkClient();
            try {
                // FIXME can't figure out where this is retained
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

            if (DEFAULT_STORAGE_DIR.exists()) {
                TestUtils.deleteDirectoryContents(DEFAULT_STORAGE_DIR);
            }
            DEFAULT_STORAGE_DIR.mkdirs();

            // 1. instantiate S4 app
            initializeS4App(getClass());

            // 2. generate a simple event that creates and changes the state of
            // the
            // PE

            // NOTE: coordinate through zookeeper
            final CountDownLatch signalValue1Set = new CountDownLatch(1);

            TestUtils.watchAndSignalCreation("/value1Set", signalValue1Set, zk);
            EventGenerator gen = new EventGenerator();
            gen.injectValueEvent(new KeyValue("value1", "message1"), "Stream1",
                    0);

            signalValue1Set.await();
            StatefulTestPE pe = (StatefulTestPE) S4TestCase.registeredPEs
                    .get(new SafeKeeperId("Stream1", "statefulPE",
                            StatefulTestPE.class.getName(), null));
            Assert.assertEquals("message1", pe.getValue1());
            Assert.assertEquals("", pe.getValue2());

            // 3. generate a checkpoint event
            final CountDownLatch signalCheckpointed = new CountDownLatch(1);
            TestUtils.watchAndSignalCreation("/checkpointed",
                    signalCheckpointed, zk);
            pe.initiateCheckpoint();
            signalCheckpointed.await();

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
            KryoSerDeser kryoSerDeser = new KryoSerDeser();
            byte[] refBytes = kryoSerDeser.serialize(refPE);

            Assert.assertTrue(Arrays.equals(refBytes,
                    TestUtils.readFileAsByteArray(expected)));


        } finally {
            TestUtils.stopZookeeperServer(zookeeperServerConnectionFactory);
        }
    }

}
