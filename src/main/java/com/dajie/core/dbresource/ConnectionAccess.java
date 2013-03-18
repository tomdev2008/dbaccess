package com.dajie.core.dbresource;

import java.sql.Connection;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public interface ConnectionAccess {

    /**
     * 
     * @return
     * @throws Exception
     */
    public Connection getReadConnection() throws Exception;

    /**
     * 
     * @param pattern
     * @return
     * @throws Exception
     */
    public Connection getReadConnection(String pattern) throws Exception;

    /**
     * 
     * @return
     * @throws Exception
     */
    public Connection getWriteConnection() throws Exception;

    /**
     * 
     * @param pattern
     * @return
     * @throws Exception
     */
    public Connection getWriteConnection(String pattern) throws Exception;
}
