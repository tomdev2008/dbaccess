package sirius.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import sirius.cache.exception.CacheException;
import sirius.util.Continuum;
import sirius.util.SerializeUtil;
import sirius.zkclient.ZkClient;
import sirius.zkclient.exception.ZkException;
import sirius.zkclient.listener.NodeDataListener;

/**
 * 客户端使用的类
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class CacheClient extends NodeDataListener implements CacheAccess {

    private static Logger logger = Constant.logger;

    private String namespace;

    private String business;

    private Continuum continuum;

    private Map<String, JedisPool> jedisPoolMap; // nick to pool

    private ReentrantReadWriteLock rwLock;

    private ZkClient zkClient;

    public CacheClient(String namespace, String business) {
        super(Constant.DEFAULT_CACHE_PREFIX + Constant.DIR_SEPARATOR + namespace);
        this.namespace = namespace;
        this.business = business;
        this.continuum = new Continuum(business);
        jedisPoolMap = new HashMap<String, JedisPool>();
        rwLock = new ReentrantReadWriteLock();
        zkClient = ZkClient.getInstance(Constant.DEFAULT_ZK_ADDRESS);
        zkClient.addNodeDataListener(this);
        try { //register event 
            zkClient.exist(getNodePath(), true);
        } catch (ZkException e) {
            logger.error(e);
        }
        update(getNodePath());
    }

    private static String generateCacheKey(String prefix, String key) {
        return prefix + Constant.SEPARATOR + key;
    }

    protected JedisPool locateJedisPool(String key) {
        String nickname = continuum.locate(Continuum.hash(key));
        ReadLock rlock = rwLock.readLock();
        try {
            rlock.lock();
            return jedisPoolMap.get(nickname);
        } finally {
            rlock.unlock();
        }
    }

    protected String description(String msg) {
        StringBuffer sb = new StringBuffer();
        sb.append("[ns:").append(namespace).append(",biz:").append(business);
        if (msg != null) {
            sb.append(", msg:").append(msg);
        }
        sb.append("]");
        return sb.toString();
    }


    /**
     * 取对象
     * 
     * @param key
     * @return
     * @throws CacheException
     */
    public Object getObject(String key) throws CacheException {
        Object obj = null;
        byte[] byteVal = get(key);
        if (byteVal != null && byteVal.length != 0) {
            try {
                obj = SerializeUtil.deserialize(byteVal);
            } catch (Exception e) {
                throw new CacheException(description("CacheClient.getObject() key:" + key));
            }
        }
        return obj;
    }

    @Override
    public byte[] get(String key) throws CacheException {
        String cacheKey = generateCacheKey(business, key);
        JedisPool pool = locateJedisPool(key);
        if (pool == null) {
            logger.error("CacheClient.get() cannot get JedisPool! key:" + key);
            return null;
        }
        Jedis jedis = null;
        byte[] value = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                value = jedis.get(cacheKey.getBytes());
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                try {
                    pool.returnBrokenResource(jedis);
                } catch (Exception ex) {
                    throw new CacheException(description(ex.getMessage()), ex);
                }
            }
            throw new CacheException(description(e.getMessage()), e);
        }
        return value;
    }

    /**
     * 写对象接口， Object必要是可序列化的
     * 
     * @param key
     * @param val
     * @param expireTime
     * @return
     * @throws CacheException
     */
    public boolean setObject(String key, Object val, int expireTime) throws CacheException {
        if (val == null) {
            return false;
        }
        try {
            byte[] byteVal = SerializeUtil.serialize(val);
            return set(key, byteVal, expireTime);
        } catch (Exception e) {
            logger.error("CacheClient.setObject() key:" + key + "\tvalue:" + val + "\texpireTime:"
                    + expireTime);
            throw new CacheException(description(e.getMessage()), e);
        }
    }

    @Override
    public boolean set(String key, byte[] value, int expireTime) throws CacheException {
        String cacheKey = generateCacheKey(business, key);
        JedisPool pool = locateJedisPool(key);
        if (pool == null) {
            logger.error("CacheClient.get() cannot get JedisPool! key:" + key);
            return false;
        }
        Jedis jedis = null;
        String okay = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                okay = jedis.setex(cacheKey.getBytes(), expireTime, value);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                try {
                    pool.returnBrokenResource(jedis);
                } catch (Exception ex) {
                    throw new CacheException(description(ex.getMessage()), ex);
                }
            }
            throw new CacheException(description(e.getMessage()), e);
        }
        return Constant.OK.equals(okay);
    }

    @Override
    public Map<String, byte[]> multiGet(List<String> keys) throws CacheException {
        //TODO
        return null;
    }

    @Override
    public boolean exists(String key) throws CacheException {
        String cacheKey = generateCacheKey(business, key);
        JedisPool pool = locateJedisPool(key);
        if (pool == null) {
            logger.error("CacheClient.exists() cannot get JedisPool! key:" + key);
            return false;
        }
        Jedis jedis = null;
        boolean res = false;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                res = jedis.exists(cacheKey.getBytes());
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                try {
                    pool.returnBrokenResource(jedis);
                } catch (Exception ex) {
                    throw new CacheException(description(ex.getMessage()), ex);
                }
            }
            throw new CacheException(description(e.getMessage()), e);
        }
        return res;
    }

    @Override
    public Long strlen(String key) throws CacheException {
        String cacheKey = generateCacheKey(business, key);
        JedisPool pool = locateJedisPool(key);
        if (pool == null) {
            logger.error("CacheClient.strlen() cannot get JedisPool! key:" + key);
            return -1L;
        }
        Jedis jedis = null;
        Long len = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                len = jedis.strlen(cacheKey.getBytes());

            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                try {
                    pool.returnBrokenResource(jedis);
                } catch (Exception ex) {
                    throw new CacheException(description(ex.getMessage()), ex);
                }
            }
            throw new CacheException(description(e.getMessage()), e);
        }
        return len;
    }

    @Override
    public boolean delete(String key) throws CacheException {
        String cacheKey = generateCacheKey(business, key);
        JedisPool pool = locateJedisPool(key);
        if (pool == null) {
            logger.error("CacheClient.delete() cannot get JedisPool! key:" + key);
            return false;
        }
        Jedis jedis = null;
        Long res = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                res = jedis.del(cacheKey.getBytes());
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                try {
                    pool.returnBrokenResource(jedis);
                } catch (Exception ex) {
                    throw new CacheException(description(ex.getMessage()), ex);
                }
            }
            throw new CacheException(description(e.getMessage()), e);
        }
        return res.equals(Constant.SUCCESS);
    }

    /**
     * delete znodepath event with empty implementation
     */
    @Override
    public boolean delete() {
        logger.warn(description("CacheClient.delete() znodepath is deleted"));
        return true;
    }

    @Override
    public boolean update(String nodePath) {
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
                logger.error(e);
            }
            if (nodes != null) {
                Continuum cm = new Continuum(business);
                Map<String, JedisPool> newPoolMap = new HashMap<String, JedisPool>();
                for (String node : nodes) {
                    // name:host:port:pwd:timeout
                    String[] fields = node.split(Constant.SEPARATOR);
                    String nickname = fields[0];
                    String host = fields[1];
                    String password = fields[3];
                    int port;
                    int timeout;
                    try {
                        port = Integer.parseInt(fields[2]);
                        timeout = Integer.parseInt(fields[4]);
                    } catch (Exception e) {
                        logger.error(e);
                        continue;
                    }
                    JedisPool pool = JedisPoolFactory.getPool(host, port, timeout, password);
                    newPoolMap.put(nickname, pool);
                    cm.add(nickname, 100);
                }
                if (!newPoolMap.isEmpty()) {
                    cm.rebuild();
                    this.continuum = cm;
                    this.jedisPoolMap.clear();
                    this.jedisPoolMap.putAll(newPoolMap);
                }
            }

            logger.debug("continuum.size():" + continuum.size());
            logger.debug("jedisPoolMap.size():" + jedisPoolMap.size());

        } finally {
            wlock.unlock();
        }
        return true;
    }

    @Override
    public boolean setLong(String key, long value, int expireTime) throws CacheException {
        return set(key, String.valueOf(value).getBytes(), expireTime);
    }

    @Override
    public long getLong(String key) throws CacheException {
        long res = Constant.ERROR_COUNT;
        byte[] byteVal = get(key);
        if (byteVal != null && byteVal.length != 0) {
            try {
                res = Long.parseLong(new String(byteVal));
            } catch (Exception e) {
                logger.error(e);
                throw new CacheException(description(e.getMessage()), e);
            }
        }
        return res;
    }

    @Override
    public long incLong(String key, long step) throws CacheException {
        long res = Constant.ERROR_COUNT;
        String cacheKey = generateCacheKey(business, key);
        JedisPool pool = locateJedisPool(key);
        if (pool == null) {
            logger.error("CacheClient.incLong() cannot get JedisPool! key:" + key + "\tstep:"
                    + step);
            return res;
        }
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
            if (jedis != null) {
                res = jedis.incrBy(cacheKey.getBytes(), step);
            }
            pool.returnResource(jedis);
        } catch (Exception e) {
            if (jedis != null) {
                try {
                    pool.returnBrokenResource(jedis);
                } catch (Exception ex) {
                    throw new CacheException(description(ex.getMessage()), ex);
                }
            }
            throw new CacheException(description(e.getMessage()), e);
        }
        return res;
    }
    /**
    public static void main(String[] args) {
        BasicConfigurator.configure();
        CacheClient cc = new CacheClient("namespace", "hotel");
        
        try {
            TimeUnit.HOURS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    */
}
