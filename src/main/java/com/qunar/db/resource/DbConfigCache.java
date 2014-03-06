package com.qunar.db.resource;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qunar.db.util.DbLogger;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class DbConfigCache {

    private static final Logger logger = LoggerFactory.getLogger(DbLogger.class);

    // namespace to DbConfig map
    private final static Map<String, DbConfig> dbConfigMap = new HashMap<String, DbConfig>();

    public static DbConfig getConfig(String ns, String cipher) {
        if ("".equals(ns)) {
            logger.error("DbConfigCache.getConfig() invalid input argv(ns):" + ns);
            return null;
        }
        synchronized (DbConfigCache.class) {
            DbConfig config = dbConfigMap.get(ns);
            if (config == null) {
                config = new DbConfig(ns, cipher);
                dbConfigMap.put(ns, config);
            }
            return config;
        }
    }
}
