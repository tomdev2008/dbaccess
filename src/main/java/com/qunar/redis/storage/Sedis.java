package com.qunar.redis.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

import com.qunar.redis.storage.exception.CacheException;
import com.qunar.redis.util.Continuum;
import com.qunar.zkclient.ZkClient;
import com.qunar.zkclient.exception.ZkException;
import com.qunar.zkclient.listener.NodeDataListener;

public class Sedis implements JedisCommands {

    public final static int HASH_ELEMENT_MAX_SIZE = 5000;

    private final static Logger logger = Constant.logger;

    private final String namespace;

    private final String cipher;

    private final Impl impl;

    public Sedis(String namespace) {
        this(namespace, "");
    }

    public Sedis(String namespace, String cipher) {
        this.namespace = namespace;
        this.cipher = cipher;
        impl = new Impl(Constant.DEFAULT_STORAGE_PREFIX + Constant.DIR_SEPARATOR + namespace);
    }

    private static String generateCacheKey(String prefix, String key) {
        return key;
    }

    private static byte[] generateCacheKey(String prefix, byte[] key) {
        return key;
    }

    protected String description(String msg) {
        StringBuffer sb = new StringBuffer();
        sb.append("[ns:").append(namespace);
        if (msg != null) {
            sb.append(", msg:").append(msg);
        }
        sb.append("]");
        return sb.toString();
    }

    private class Impl extends NodeDataListener {

        private Continuum continuum;

        private Map<String, List<JedisPool>> poolMap;

        private Random rand; //randomize to get jedispool

        private ReentrantReadWriteLock rwLock;

        private ZkClient zkClient;

        Impl(String path) {
            super(path);
            this.continuum = new Continuum(namespace);
            poolMap = new HashMap<String, List<JedisPool>>();
            rand = new Random(System.currentTimeMillis());
            rwLock = new ReentrantReadWriteLock();
            zkClient = ZkClient.getInstance(Constant.DEFAULT_ZK_ADDRESS);
            zkClient.addNodeDataListener(this);
            try { //register event 
                zkClient.exist(getNodePath(), true);
            } catch (ZkException e) {
                logger.error(e.getMessage(), e);
            }
            update(getNodePath());
        }

        protected JedisPool locateJedisPool(String key) {
            String nickname = continuum.locate(Continuum.hash(key));
            ReadLock rlock = rwLock.readLock();
            try {
                rlock.lock();
                List<JedisPool> pools = poolMap.get(nickname);
                if (pools == null || pools.isEmpty()) {
                    return null;
                } else {
                    int size = pools.size();
                    if (size == 1) { //needn't to get randomly
                        return pools.get(0);
                    } else {
                        return pools.get(rand.nextInt(size));
                    }

                }
            } finally {
                rlock.unlock();
            }
        }

        protected JedisPool locateJedisPool(byte[] key) {
            String nickname = continuum.locate(Continuum.hash(key, key.length));
            ReadLock rlock = rwLock.readLock();
            try {
                rlock.lock();
                List<JedisPool> pools = poolMap.get(nickname);
                if (pools == null || pools.isEmpty()) {
                    return null;
                } else {
                    int size = pools.size();
                    if (size == 1) { //needn't to get randomly
                        return pools.get(0);
                    } else {
                        return pools.get(rand.nextInt(size));
                    }

                }
            } finally {
                rlock.unlock();
            }
        }

        @Override
        public boolean update(String value) {
            WriteLock wlock = rwLock.writeLock();
            try {
                wlock.lock();
                if (zkClient == null) {
                    logger.error("zkClient is null!");
                    return false;
                }

                List<String> nodes = null;
                try {
                    nodes = zkClient.getChildren(getNodePath(), false);
                } catch (ZkException e) {
                    logger.error(e.getMessage(), e);
                }
                if (nodes != null) {
                    Continuum cm = new Continuum(namespace);
                    Map<String, List<JedisPool>> newPoolMap = new HashMap<String, List<JedisPool>>();
                    for (String node : nodes) {
                        String[] fields = node.split(Constant.SEPARATOR);
                        String nickname = fields[0];
                        String host = fields[1];
                        //String password = fields[3];
                        String password = cipher;
                        int port;
                        int timeout;
                        try {
                            port = Integer.parseInt(fields[2]);
                            timeout = Integer.parseInt(fields[4]);
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                            continue;
                        }
                        JedisPool pool = JedisPoolFactory.getPool(host, port, timeout, password);
                        if (newPoolMap.containsKey(nickname)) {
                            newPoolMap.get(nickname).add(pool);
                        } else {
                            List<JedisPool> pools = new ArrayList<JedisPool>();
                            pools.add(pool);
                            newPoolMap.put(nickname, pools);
                            cm.add(nickname, 100);
                        }
                    }
                    if (!newPoolMap.isEmpty()) {
                        cm.rebuild();
                        this.continuum = cm;
                        this.poolMap.clear();
                        this.poolMap.putAll(newPoolMap);
                    }
                }

                logger.debug(description("continuum.size():" + continuum.size()));
                logger.debug(description("poolMap.size():" + poolMap.size()));
                logger.debug(description(poolMap.toString()));
            } finally {
                wlock.unlock();
            }
            return true;
        }

        @Override
        public boolean delete() {
            return true;
        }

    }

    /* binary commands start*/

    public byte[] get(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(cacheKey);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        byte[] value = null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.get(key);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    public String set(byte[] key, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String ok = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.set(cacheKey, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;

    }

    public Boolean exists(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean ex = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ex = jedis.exists(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ex;
    }

    public Long persist(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long ok = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.persist(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;

    }

    public String type(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String type = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                type = jedis.type(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return type;
    }

    public Long expire(byte[] key, int seconds) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long on = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                on = jedis.expire(cacheKey, seconds);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return on;
    }

    public Long expireAt(byte[] key, long unixTime) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long on = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                on = jedis.expireAt(cacheKey, unixTime);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return on;
    }

    public Long ttl(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long ttl = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ttl = jedis.ttl(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ttl;
    }

    public String setex(byte[] key, int seconds, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String ok = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.setex(cacheKey, seconds, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    public Boolean setbit(byte[] key, long offset, boolean value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean origin = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                origin = jedis.setbit(cacheKey, offset, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return origin;
    }

    public Boolean setbit(byte[] key, long offset, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean origin = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                origin = jedis.setbit(cacheKey, offset, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return origin;
    }

    public Boolean getbit(byte[] key, long offset) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean bt = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                bt = jedis.getbit(cacheKey, offset);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return bt;
    }

    public Long setrange(byte[] key, long offset, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long len = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.setrange(cacheKey, offset, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        byte[] value = null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.getrange(cacheKey, startOffset, endOffset);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    public byte[] getSet(byte[] key, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        byte[] pre = null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                pre = jedis.getSet(cacheKey, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return pre;
    }

    public Long setnx(byte[] key, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long ok = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.setnx(cacheKey, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    public Long decrBy(byte[] key, long step) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long value = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.decrBy(cacheKey, step);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    public Long decr(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long value = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.decr(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    public Long incrBy(byte[] key, long step) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long v = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                v = jedis.incrBy(cacheKey, step);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return v;
    }

    public Long incr(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long v = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                v = jedis.incr(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return v;
    }

    public Long append(byte[] key, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long len = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.append(cacheKey, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    public Long del(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removedSize = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removedSize = jedis.del(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removedSize;
    }

    public Long hset(byte[] key, byte[] field, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long ok = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.hset(cacheKey, field, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    public byte[] hget(byte[] key, byte[] field) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        byte[] value = null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.hget(cacheKey, field);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long ok = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.hsetnx(cacheKey, field, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String ok = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.hmset(cacheKey, hash);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        List<byte[]> values = new ArrayList<byte[]>();
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                values.addAll(jedis.hmget(cacheKey, fields));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return values;
    }

    public Long hincrBy(byte[] key, byte[] field, long value) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long v = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                v = jedis.hincrBy(cacheKey, field, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return v;

    }

    public Boolean hexists(byte[] key, byte[] field) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean ex = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ex = jedis.hexists(cacheKey, field);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ex;
    }

    public Long hdel(byte[] key, byte[]... fields) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removedSize = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removedSize = jedis.hdel(cacheKey, fields);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removedSize;
    }

    public Long hlen(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long len = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.hlen(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    public Set<byte[]> hkeys(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Set<byte[]> keys = null;;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                keys = jedis.hkeys(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return keys;
    }

    public Collection<byte[]> hvals(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        List<byte[]> values = new ArrayList<byte[]>();
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                values.addAll(jedis.hvals(cacheKey));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return values;

    }

    public Map<byte[], byte[]> hgetAll(byte[] key) {
        byte[] cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Map<byte[], byte[]> value = new HashMap<byte[], byte[]>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.hgetAll(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        if (value.size() >= HASH_ELEMENT_MAX_SIZE) {
            logger.warn(description("key:" + key
                    + ", type:hash, contains two many (field, value) elements"));
        }
        return value;

    }

    /* binary commands end */

    @Override
    public Long append(String key, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long len = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.append(cacheKey, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public Long bitcount(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long count = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                count = jedis.bitcount(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return count;
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long count = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                count = jedis.bitcount(cacheKey, start, end);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return count;
    }

    @Override
    public List<String> blpop(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        List<String> list = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                list = jedis.blpop(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return list;
    }

    @Override
    public List<String> brpop(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        List<String> list = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                list = jedis.brpop(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return list;
    }

    @Override
    public Long decr(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long value = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.decr(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public Long decrBy(String key, long step) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long value = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.decrBy(cacheKey, step);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public Long del(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removedSize = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removedSize = jedis.del(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removedSize;
    }

    @Override
    public String echo(String str) {
        // useless
        return str;
    }

    @Override
    public Boolean exists(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean ex = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ex = jedis.exists(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ex;
    }

    @Override
    public Long expire(String key, int seconds) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long on = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                on = jedis.expire(cacheKey, seconds);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return on;
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long on = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                on = jedis.expireAt(cacheKey, unixTime);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return on;
    }

    @Override
    public String get(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String value = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.get(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public String getSet(String key, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String pre = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                pre = jedis.getSet(cacheKey, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return pre;
    }

    @Override
    public Boolean getbit(String key, long offset) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean bt = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                bt = jedis.getbit(cacheKey, offset);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return bt;
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String value = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.getrange(cacheKey, startOffset, endOffset);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public Long hdel(String key, String... fields) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removedSize = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removedSize = jedis.hdel(cacheKey, fields);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removedSize;
    }

    @Override
    public Boolean hexists(String key, String field) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean ex = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ex = jedis.hexists(cacheKey, field);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ex;
    }

    @Override
    public String hget(String key, String field) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String value = null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.hget(cacheKey, field);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Map<String, String> value = new HashMap<String, String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.hgetAll(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        if (value.size() >= HASH_ELEMENT_MAX_SIZE) {
            logger.warn(description("key:" + key
                    + ", type:hash, contains two many (field, value) elements"));
        }
        return value;
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long v = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                v = jedis.hincrBy(cacheKey, field, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return v;
    }

    @Override
    public Set<String> hkeys(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Set<String> keys = null;;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                keys = jedis.hkeys(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return keys;
    }

    @Override
    public Long hlen(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long len = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.hlen(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        List<String> values = new ArrayList<String>();
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                values.addAll(jedis.hmget(cacheKey, fields));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return values;
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String ok = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.hmset(cacheKey, hash);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public Long hset(String key, String field, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long ok = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.hset(cacheKey, field, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long ok = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.hsetnx(cacheKey, field, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public List<String> hvals(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        List<String> values = new ArrayList<String>();
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                values.addAll(jedis.hvals(cacheKey));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return values;
    }

    @Override
    public Long incr(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long v = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                v = jedis.incr(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return v;
    }

    @Override
    public Long incrBy(String key, long step) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long v = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                v = jedis.incrBy(cacheKey, step);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return v;
    }

    @Override
    public String lindex(String key, long index) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        String value = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.lindex(cacheKey, index);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long len = -1L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.linsert(cacheKey, where, pivot, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public Long llen(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long len = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.llen(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public String lpop(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        String value = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.lpop(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public Long lpush(String key, String... strings) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long len = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.lpush(cacheKey, strings);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public Long lpushx(String key, String... strings) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long len = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.lpushx(cacheKey, strings);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        List<String> values = new ArrayList<String>();
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                values.addAll(jedis.lrange(cacheKey, start, end));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return values;
    }

    @Override
    public Long lrem(String key, long count, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long removed = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removed = jedis.lrem(cacheKey, count, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removed;
    }

    @Override
    public String lset(String key, long index, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        String ok = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.lset(cacheKey, index, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public String ltrim(String key, long start, long end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        String ok = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.ltrim(cacheKey, start, end);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public Long move(String arg0, int arg1) {
        // 不用实现
        return null;
    }

    @Override
    public Long persist(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long ok = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.persist(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public String rpop(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        String value = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.rpop(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public Long rpush(String key, String... strings) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long len = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.rpush(cacheKey, strings);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public Long rpushx(String key, String... strings) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long len = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.rpushx(cacheKey, strings);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public Long sadd(String key, String... members) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long addSize = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                addSize = jedis.sadd(cacheKey, members);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return addSize;
    }

    @Override
    public Long scard(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Jedis jedis = null;
        Long size = 0L;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                size = jedis.scard(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return size;
    }

    @Override
    public String set(String key, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String ok = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.set(cacheKey, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean origin = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                origin = jedis.setbit(cacheKey, offset, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return origin;
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean origin = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                origin = jedis.setbit(cacheKey, offset, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return origin;
    }

    @Override
    public String setex(String key, int seconds, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String ok = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.setex(cacheKey, seconds, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public Long setnx(String key, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long ok = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.setnx(cacheKey, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long len = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.setrange(cacheKey, offset, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public Boolean sismember(String key, String member) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Boolean ok = false;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ok = jedis.sismember(cacheKey, member);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ok;
    }

    @Override
    public Set<String> smembers(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> members = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                members.addAll(jedis.smembers(cacheKey));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return members;
    }

    @Override
    public List<String> sort(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        List<String> sorted = new ArrayList<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                sorted.addAll(jedis.sort(cacheKey));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return sorted;
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        List<String> sorted = new ArrayList<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                sorted.addAll(jedis.sort(cacheKey, sortingParameters));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return sorted;
    }

    @Override
    public String spop(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String value = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.spop(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public String srandmember(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String value = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.srandmember(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return value;
    }

    @Override
    public Long srem(String key, String... members) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removed = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removed = jedis.srem(cacheKey, members);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removed;
    }

    @Override
    public Long strlen(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long len = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.strlen(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public String substr(String key, int start, int end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String sub = null;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                sub = jedis.substr(cacheKey, start, end);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return sub;
    }

    @Override
    public Long ttl(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long ttl = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ttl = jedis.ttl(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ttl;
    }

    @Override
    public String type(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        String type = "";
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                type = jedis.type(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return type;
    }

    @Override
    public Long zadd(String key, Map<Double, String> scoreMembers) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long added = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                added = jedis.zadd(cacheKey, scoreMembers);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return added;
    }

    @Override
    public Long zadd(String key, double score, String member) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long added = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                added = jedis.zadd(cacheKey, score, member);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return added;
    }

    @Override
    public Long zcard(String key) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long len = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.zcard(cacheKey);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return len;
    }

    @Override
    public Long zcount(String key, double min, double max) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long count = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                count = jedis.zcount(cacheKey, min, max);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return count;
    }

    @Override
    public Long zcount(String key, String min, String max) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long count = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                count = jedis.zcount(cacheKey, min, max);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return count;
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Double ns = 0d;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                ns = jedis.zincrby(cacheKey, score, member);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return ns;

    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrange(cacheKey, start, end));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<String> zrangeByScore(String key, double start, double end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeByScore(cacheKey, start, end));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeByScore(cacheKey, min, max));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeByScore(cacheKey, min, max, offset, count));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeByScore(cacheKey, min, max, offset, count));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeByScoreWithScores(cacheKey, min, max));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeByScoreWithScores(cacheKey, min, max));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset,
            int count) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeByScoreWithScores(cacheKey, min, max, offset, count));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset,
            int count) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeByScoreWithScores(cacheKey, min, max, offset, count));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrangeWithScores(cacheKey, start, end));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Long zrank(String key, String member) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long rank = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                rank = jedis.zrank(cacheKey, member);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return rank;
    }

    @Override
    public Long zrem(String key, String... members) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removed = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removed = jedis.zrem(cacheKey, members);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removed;
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removed = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removed = jedis.zremrangeByRank(cacheKey, start, end);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removed;
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removed = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removed = jedis.zremrangeByScore(cacheKey, start, end);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removed;
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long removed = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                removed = jedis.zremrangeByScore(cacheKey, start, end);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return removed;
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrange(cacheKey, start, end));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeByScore(cacheKey, max, min));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeByScore(cacheKey, max, min));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeByScore(cacheKey, max, min, offset, count));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<String> range = new HashSet<String>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeByScore(cacheKey, max, min, offset, count));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeByScoreWithScores(cacheKey, max, min));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeByScoreWithScores(cacheKey, max, min));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset,
            int count) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeByScoreWithScores(cacheKey, max, min, offset, count));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset,
            int count) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeByScoreWithScores(cacheKey, max, min, offset, count));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Set<Tuple> range = new HashSet<Tuple>();
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                range.addAll(jedis.zrevrangeWithScores(cacheKey, start, end));
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return range;
    }

    @Override
    public Long zrevrank(String key, String member) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Long rank = 0L;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                rank = jedis.zrevrank(cacheKey, member);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return rank;
    }

    @Override
    public Double zscore(String key, String member) {
        String cacheKey = generateCacheKey("", key);
        JedisPool pool = impl.locateJedisPool(key);
        if (pool == null) {
            logger.error(description("Not available JedisPool"));
            return null;
        }
        Double score = 0d;
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                score = jedis.zscore(cacheKey, member);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                pool.returnBrokenResource(jedis);
            }
            throw new CacheException(e);
        }
        return score;
    }

    /*
    public static void main(String[] args) {
        BasicConfigurator.configure();
        Sedis sedis = new Sedis("dba_test_rw", "13a76724");
        byte[] key = new byte[] { 'm', 'y' };
        byte[] value = new byte[] { 'v', 'x', 'y', 'z', 'v' };
        System.out.println(sedis.setex(key, 100, value));
        System.out.println(sedis.set(key, value));
        System.out.println(Arrays.toString(sedis.get(key)));
        System.out.println(sedis.exists(key));
        System.out.println(sedis.ttl(key));
        System.out.println(sedis.persist(key));
        System.out.println(sedis.ttl(key));
        System.out.println(sedis.type(key));
        System.out.println(sedis.expire(key, 100));
        System.out.println(sedis.ttl(key));
        System.out.println(sedis.expireAt(key, System.currentTimeMillis() + 2000));
        System.out.println(sedis.ttl(key));
        System.out.println(sedis.del(key));
        System.out.println(sedis.exists(key));
    }
    */
}
