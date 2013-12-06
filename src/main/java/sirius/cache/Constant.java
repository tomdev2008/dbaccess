package sirius.cache;

import org.apache.log4j.Logger;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public interface Constant {

    final String DEFAULT_CACHE_PREFIX = "/cache";

    final String DIR_SEPARATOR = "/";

    final String SEPARATOR = ":";

    final Logger logger = Logger.getLogger(sirius.util.CacheLogger.class);

    final String DEFAULT_ZK_ADDRESS = "127.0.0.1:2181";

    final String OK = "OK";

    final Long SUCCESS = 1L;

}
