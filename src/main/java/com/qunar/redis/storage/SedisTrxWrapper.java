package com.qunar.redis.storage;

import java.io.Closeable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import com.qunar.redis.storage.exception.CacheException;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class SedisTrxWrapper implements Closeable {

    private Jedis jedis;

    private JedisPool pool;

    public SedisTrxWrapper(Jedis jedis, JedisPool pool) {
        this.jedis = jedis;
        this.pool = pool;
    }

    public Transaction multi() {
        return jedis.multi();
    }

    public String watch(final String... keys) {
        return jedis.watch(keys);
    }

    public String watch(final byte[]... keys) {
        return jedis.watch(keys);
    }

    public String unwatch() {
        return jedis.unwatch();
    }

    @Override
    public void close() throws CacheException {
        pool.returnResource(jedis);
    }

}
