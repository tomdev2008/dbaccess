package com.qunar.db.resource;

import java.sql.Connection;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public interface ConnectionAccess {

    final String DATABASE_DESC_PREFIX = "/database_desc/";

    final String READ_FLAG = "R";

    final String WRITE_FLAG = "W";

    /**
     * final Logger logger =
     * LoggerFactory.getLogger(com.qunar.db.util.DbLogger.class);
     * 
     * 
     * 
     * final String PATTERN = "pattern";
     * 
     * final String HOST = "host";
     * 
     * final String PORT = "port";
     * 
     * final String USER = "user";
     * 
     * final String PASSWORD = "password";
     * 
     * final String FLAG = "flag";
     * 
     * final String DB_NAME = "db_name";
     * 
     * final String CORE_SIZE = "core_size";
     * 
     * final String MAX_SIZE = "max_size";
     * 
     * final String READ_FLAG = "R";
     * 
     * final String WRITE_FLAG = "W";
     * 
     * final String RW_FLAG = "RW";
     */
    /**
     * read single mode
     * 
     * @return
     * @throws Exception
     */
    public Connection getReadConnection() throws Exception;

    /**
     * read router mode
     * 
     * @param pattern
     * @return
     * @throws Exception
     */
    public Connection getReadConnection(String pattern) throws Exception;

    /**
     * write single mode
     * 
     * @return
     * @throws Exception
     */
    public Connection getWriteConnection() throws Exception;

    /**
     * wirte router mode
     * 
     * @param pattern
     * @return
     * @throws Exception
     */
    public Connection getWriteConnection(String pattern) throws Exception;
}
