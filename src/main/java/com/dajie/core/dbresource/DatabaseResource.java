package com.dajie.core.dbresource;

import java.sql.Connection;

/**
 * 
 * @author yong.li@dajie-inc.com
 *
 */
public interface DatabaseResource {

	public Connection getReadConnection(String pattern);
	
	public Connection getWriteConnection(String pattern);
}
