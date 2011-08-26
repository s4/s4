package io.s4.ft;

import io.s4.ft.SafeKeeper.StorageResultCode;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * <p>
 * This class implements a storage backend based on Redis. Redis is a key-value
 * store.
 * </p>
 * <p>
 * See {@link http://redis.io/} for more information.
 * </p>
 * <p>
 * Redis must be running as an external service. References to this external
 * services must be injected during the initialization of the S4 platform.
 * </p>
 * 
 * 
 */
public class RedisStateStorage implements StateStorage {

    static Logger logger = Logger.getLogger("s4-ft");
    private JedisPool jedisPool;
    private String redisHost;
    private int redisPort;
    private ThreadPoolExecutor threadPool;
    // TODO: should probably define a lower default value...
    int maxWriteThreads = Runtime.getRuntime().availableProcessors() == 1 ? 1 : (Runtime.getRuntime()
            .availableProcessors() - 1);
    int writeThreadKeepAliveSeconds = 120;
    int maxOutstandingWriteRequests = 1000;

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

    public void clear() {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.flushAll();
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    public void init() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // TODO optional parameterization
        jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort);
        threadPool = new ThreadPoolExecutor(0, maxWriteThreads, writeThreadKeepAliveSeconds, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(maxOutstandingWriteRequests));
    }

    @Override
    public void saveState(SafeKeeperId key, byte[] state, StorageCallback callback) {
        try {
            threadPool.submit(new SaveTask(key, state, callback, jedisPool));
        } catch (RejectedExecutionException e) {
            callback.storageOperationResult(StorageResultCode.FAILURE,
                    "Could not submit task to persist checkpoint. Remaining capacity for task queue is ["
                            + threadPool.getQueue().remainingCapacity() + "] ; number of elements is ["
                            + threadPool.getQueue().size() + "] ; maximum capacity is [" + maxOutstandingWriteRequests
                            + "]");
        }
    }

    @Override
    public byte[] fetchState(SafeKeeperId key) {
        Jedis jedis = jedisPool.getResource();
        try {
            return jedis.get(key.getStringRepresentation().getBytes());
        } finally {
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public Set<SafeKeeperId> fetchStoredKeys() {
        Jedis jedis = jedisPool.getResource();
        try {
            Set<String> keys = jedis.keys("*");
            Set<SafeKeeperId> result = new HashSet<SafeKeeperId>(keys.size());
            for (String s : keys)
                result.add(new SafeKeeperId(s));
            return result;
        } finally {
            jedisPool.returnResource(jedis);
        }

    }

    private static class SaveTask implements Runnable {
        SafeKeeperId key;
        byte[] state;
        StorageCallback callback;
        JedisPool jedisPool;

        public SaveTask(SafeKeeperId key, byte[] state, StorageCallback callback, JedisPool jedisPool) {
            super();
            this.key = key;
            this.state = state;
            this.callback = callback;
            this.jedisPool = jedisPool;
        }

        public void run() {
            Jedis jedis = jedisPool.getResource();
            String statusCode = "UNKNOWN";
            try {
                statusCode = jedis.set(key.getStringRepresentation().getBytes(), state);
            } finally {
                jedisPool.returnResource(jedis);
            }
            if ("OK".equals(statusCode)) {
                callback.storageOperationResult(StorageResultCode.SUCCESS, "Redis result code is [" + statusCode + "] for key [" + key.toString() +"]");
            } else {
                callback.storageOperationResult(StorageResultCode.FAILURE, "Unexpected redis result code : [" + statusCode + "] for key [" + key.toString() +"]");
            }
        }

    }
}
