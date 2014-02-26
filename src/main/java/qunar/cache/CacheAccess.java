package qunar.cache;

import java.util.List;
import java.util.Map;

import qunar.cache.exception.CacheException;

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
     * 
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
    //    Map<String, byte[]> multiGet(List<String> keys) throws CacheException;

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
     * 
     * @param key
     * @return
     * @throws CacheException
     */
    boolean delete(String key) throws CacheException;

    /** 计数器接口BEGIN */
    /** ------------------------------------------------------- */

    /**
     * 计数器初始化接口
     * 
     * @param key
     * @param value 初始值
     * @param expireTime 过期时间,必须设置过期时间，不能永不过期，否则会被认为无效
     * @throws CacheException
     */
    boolean setLong(String key, long value, int expireTime) throws CacheException;

    /**
     * 计数器读接口
     * 
     * @param key
     * @return 计数器只支持非负整数，成功返回正常值，失败返回ERROR_COUNT
     * @throws CacheException
     */
    long getLong(String key) throws CacheException;

    /**
     * 计数器增减接口 1. 计数器只支持非负整数，成功返回正常值，异常返回ERROR_COUNT 2.
     * 在调用之前，需要用[计数器初始化接口setLong]来初始化指定过期时间，否则会自动生成一个初值为0且永不过期的值 3.
     * 如果调用失败，需要用[计数器初始化接口setLong] 重新设值，否则后续incLong和get调用会一直失败！！
     * 
     * @param key
     * @param step 增量。可以是负数，值域为long
     * @return 增加后的值。失败返回ERROR_COUNT
     * @throws CacheException
     */
    long incLong(String key, long step) throws CacheException;
    /** ------------------------------------------------------- */
    /** 计数器接口END */

}
