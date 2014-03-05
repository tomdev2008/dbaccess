package com.qunar.db.resource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;
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

    public static void main(String[] args) {
        BasicConfigurator.configure();
        DbConfig config = DbConfigCache.getConfig("test2", "");
        int loop = 0;
        while (true) {
            Connection readConn = null;
            try {
                readConn = config.getReadConnection();
                logger.debug("loop=" + loop++ + "\treadConn.isValid(): " + readConn.isValid(5000)
                        + "\treadConn:" + readConn.toString());
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                if (readConn != null) {
                    try {
                        readConn.close();
                    } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }

            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }

        }
    }
}
