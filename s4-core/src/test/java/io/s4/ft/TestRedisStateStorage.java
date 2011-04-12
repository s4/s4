package io.s4.ft;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRedisStateStorage {

    private static Process redisDaemon;
    private static final String PAYLOAD = "payload";
    private RedisStateStorage storage;

    @BeforeClass
    public static void runRedis() throws IOException {
        // String cmdline = "pwd";
        String cmdline = "./src/test/resources/redis-server";
        redisDaemon = Runtime.getRuntime().exec(cmdline);
        // BufferedReader br = new BufferedReader(new InputStreamReader(redisDaemon.getInputStream()));
        // String s;
        // while ((s = br.readLine()) != null)
        // System.out.println(s);
    }

    @Before
    public void setUp() throws Exception {
        storage = new RedisStateStorage();
        storage.setRedisHost("localhost");
        storage.setRedisPort(6379);
        storage.connect();
        storage.clear();
    }

    @Test
    public void testFetchState() throws IOException, InterruptedException {
        SafeKeeperId key = new SafeKeeperId("stream", "prototype", "classname", "key");
        storage.saveState(key, PAYLOAD.getBytes(), null);
        byte[] result = storage.fetchState(key);
        String recovered = new String(result);
        assertEquals(PAYLOAD, recovered);
    }

    @Test
    public void testFetchStoredKeys() {
        Set<SafeKeeperId> fixture = new HashSet<SafeKeeperId>();
        for (int i = 0; i < 10; i++)
            fixture.add(new SafeKeeperId("stream", "prototype", "classname", "key" + i));
        for (SafeKeeperId skid : fixture)
            storage.saveState(skid, PAYLOAD.getBytes(), null);

        // retrieve the keys
        Set<SafeKeeperId> result = storage.fetchStoredKeys();
        assertEquals(fixture.size(), result.size());
        assertEquals(fixture, result);
    }

    @AfterClass
    public static void stopRedis() throws InterruptedException {
        redisDaemon.destroy();
        int exitcode = redisDaemon.waitFor();
        if (exitcode != 0)
            System.out.println("Redis exited with non zero exit code: " + exitcode);
    }
}
