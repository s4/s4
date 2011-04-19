package io.s4.ft;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.Jedis;

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
    
    @Override
    public void saveState(SafeKeeperId key, byte[] state, StorageCallback callback) {
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
