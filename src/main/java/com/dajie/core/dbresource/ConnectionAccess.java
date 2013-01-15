package com.dajie.core.dbresource;

import java.sql.Connection;

/**
 * 
 * @author yong.li@dajie-inc.com
 *
 */
public interface ConnectionAccess {

	public Connection getReadConnection(String pattern);
	
	public Connection getWriteConnection(String pattern);
}
