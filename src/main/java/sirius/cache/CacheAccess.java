package sirius.cache;

import java.util.List;
import java.util.Map;

import sirius.cache.exception.CacheException;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public interface CacheAccess {

    /**
     * get接口， 返回String,如果失败返回null
     * 
     * @param key
     * @return
     * @throws CacheException
     */
    byte[] get(String key) throws CacheException;

    /**
     * set接口 ，成功返回true,失败返回false
     * @param key
     * @param value
     * @param expireTime
     * @return
     * @throws CacheException
     */
    boolean set(String key, byte[] value, int expireTime) throws CacheException;

    /**
     * 批量get接口， 返回key-value map,如果失败对应的值返回null
     * 
     * @param key
     * @return
     * @throws CacheException
     */
    Map<String, byte[]> multiGet(List<String> keys) throws CacheException;

    /**
     * 是否存在接口, 成功返回true,失败返回false
     * 
     * @param key
     * @return
     * @throws CacheException
     */
    boolean exists(String key) throws CacheException;

    /**
     * 取value的大小接口
     * 
     * @param key
     * @return
     * @throws CacheException
     */
    Long strlen(String key) throws CacheException;
    
    /**
     * 删除接口
     * @param key
     * @return
     * @throws CacheException
     */
    boolean delete(String key) throws CacheException;
}
