package io.s4.ft;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

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
        List<String> cmdList = new ArrayList<String>();

        cmdList.add(findCompiledRedisForPlatform());
        ProcessBuilder pb = new ProcessBuilder(cmdList);
        pb.directory(new File(System.getProperty("user.dir")));
        pb.redirectErrorStream();
        redisDaemon = pb.start();
        BufferedReader br = new BufferedReader(new InputStreamReader(
                redisDaemon.getInputStream()));
        String s;
        int maxLinesBeforeOK=  4;
        for (int i = 0; i < maxLinesBeforeOK; i++) {
            if ((s=br.readLine())!=null) {
                if (s.contains("The server is now ready to accept connections on port 6379")) {
                    break;
                }
            } else {
                break;
            }
        }

        // redisDaemon = Runtime.getRuntime().exec(cmdline);
        // BufferedReader br = new BufferedReader(new
        // InputStreamReader(redisDaemon.getInputStream()));
        // String s;
        // while ((s = br.readLine()) != null)
        // System.out.println(s);
    }

    private static String findCompiledRedisForPlatform() {
        // TODO add compiled versions for other platforms/architectures
        String osName = System.getProperty("os.name");
        String osArch = System.getProperty("os.arch");
        if (osName.equalsIgnoreCase("Mac OS X")) {
            if (osArch.equalsIgnoreCase("x86_64")) {
                return "src/test/resources/macosx/redis-server-64bits";
            }
        }
        if (osName.equalsIgnoreCase("Linux")) {
            if (osArch.equalsIgnoreCase("i386")) {
                return "src/test/resources/linux/redis-server-32bits";
            }
        }
        if (!new File(System.getProperty("user.dir")
                + "/src/test/resources/redis-server").exists()) {
            Assert.fail("Could not find a redis server executable for your platform. Please place an executable redis server version compiled for your platform in s4-core/src/test/resources, named 'redis-server'");
        }
        return "src/test/resources/redis-server";

    }

    @Before
    public void setUp() throws Exception {
        storage = clearRedis();
    }

    public static RedisStateStorage clearRedis() {
        RedisStateStorage storage = new RedisStateStorage();
        storage.setRedisHost("localhost");
        storage.setRedisPort(6379);
        storage.connect();
        storage.clear();
        return storage;
    }

    @Test
    public void testFetchState() throws IOException, InterruptedException {
        SafeKeeperId key = new SafeKeeperId("prototype", "key");
        storage.saveState(key, PAYLOAD.getBytes(), null);
        byte[] result = storage.fetchState(key);
        String recovered = new String(result);
        assertEquals(PAYLOAD, recovered);
    }

    @Test
    public void testFetchStoredKeys() {
        Set<SafeKeeperId> fixture = new HashSet<SafeKeeperId>();
        for (int i = 0; i < 10; i++)
            fixture.add(new SafeKeeperId("prototype", "key" + i));
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
            System.out.println("Redis exited with non zero exit code: "
                    + exitcode);
    }
}
