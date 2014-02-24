package sirius.cache;

import org.apache.log4j.Logger;

/**
 * 常量表 <br/>
 * DEFAULT_ZK_ADDRESS需要注意，多个环境都应该有相同的域名
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public interface Constant {

    final String DEFAULT_CACHE_PREFIX = "/cache";

    final String DEFAULT_STORAGE_PREFIX = "/redis";

    final String DIR_SEPARATOR = "/";

    final String SEPARATOR = ":";

    final Logger logger = Logger.getLogger(sirius.util.CacheLogger.class);

    //    final String DEFAULT_ZK_ADDRESS = "127.0.0.1:2181";
    //    final String dEFAULT_ZK_ADDRESS = "l-zk1.dba.cn6:2181,l-zk2.dba.cn6:2181,l-zk3.dba.cn8:2181,l-zk4.dba.cn8:2181";
    final String DEFAULT_ZK_ADDRESS = "l-agdb1.dba.dev.cn6.qunar.com:2181,l-agdb2.dba.dev.cn6.qunar.com:2181,l-agdb3.dba.dev.cn6.qunar.com:2181";

    final String OK = "OK";

    final Long SUCCESS = 1L;

    final Long ERROR_COUNT = -1L;

}
