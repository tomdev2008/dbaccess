package sirius.cache.exception;

/**
 * CacheException会包装底层异常，应用层应该catch住此异常
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class StorageException extends CacheException {

    private static final long serialVersionUID = 1L;

    public StorageException(String msg) {
        super(msg);
    }

    public StorageException(String msg, Throwable t) {
        super(msg, t);
    }

    public StorageException(Throwable cause) {
        super(cause);
    }
}
