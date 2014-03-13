package com.qunar.redis.storage.exception;

/**
 * CacheException会包装底层异常，应用层应该catch住此异常
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class CacheException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public CacheException(String msg) {
        super(msg);
    }

    public CacheException(String msg, Throwable t) {
        super(msg, t);
    }

    public CacheException(Throwable cause) {
        super(cause);
    }
}
