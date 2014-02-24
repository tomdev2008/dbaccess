package sirius.cache;

import java.util.HashMap;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import sirius.util.StringUtil;

/**
 * JedisPool与(ip, port, timeout)一一对应
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class JedisPoolFactory {

    private static HashMap<String, JedisPool> poolMap = new HashMap<String, JedisPool>();

    public static JedisPool getPool(String address, int port, int timeout, String pwd) {
        synchronized (JedisPoolFactory.class) {
            String key = address + ":" + port + ":" + timeout;
            JedisPool pool = poolMap.get(key);
            if (pool == null) {
                // 一定要配置maxSize和minSize吗？ 个人感觉用默认的就成了，众口难调，不如不调！ 
                // 可以考虑(max,min)做成可动态配置的
                JedisPoolConfig config = new JedisPoolConfig();
                if (pwd == null || pwd.isEmpty()) {
                    pool = new JedisPool(config, address, port, timeout);
                } else {
                    String rPwd = StringUtil.getRedisPassword(pwd);
                    pool = new JedisPool(config, address, port, timeout, rPwd);
                }
                poolMap.put(key, pool);
            }
            return pool;
        }
    }
}
