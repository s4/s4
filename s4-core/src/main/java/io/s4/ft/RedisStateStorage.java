package io.s4.ft;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

/**
 * <p>
 * This class implements a storage backend based on Redis. Redis is a key-value store.
 * </p>
 * <p>
 * See {@link http://redis.io/} for more information.
 * </p>
 * <p>
 * Redis must be running as an external service. References to this external
 * services must be injected during the initialization of the S4 platform.
 * </p>
 */
public class RedisStateStorage implements StateStorage {
    private Jedis jedis;
    private String redisHost;

    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    private int redisPort;

    public void connect() {
        jedis = new Jedis(redisHost, redisPort);
    }

    public void clear() {
        jedis.flushAll();
    }

    public void init() {
        connect();
    }
    
    @Override
    public void saveState(SafeKeeperId key, byte[] state,
            StorageCallback callback) {
        jedis.set(key.getStringRepresentation().getBytes(), state);
    }

    @Override
    public byte[] fetchState(SafeKeeperId key) {
        return jedis.get(key.getStringRepresentation().getBytes());
    }

    @Override
    public Set<SafeKeeperId> fetchStoredKeys() {
        Set<String> keys = jedis.keys("*");
        Set<SafeKeeperId> result = new HashSet<SafeKeeperId>(keys.size());
        for (String s : keys)
            result.add(new SafeKeeperId(s));
        return result;
    }
}
