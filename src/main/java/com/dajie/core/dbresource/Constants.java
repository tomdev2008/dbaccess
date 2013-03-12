package com.dajie.core.dbresource;

import org.apache.log4j.Logger;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public interface Constants {
	//final String ZK_ADDRESS = "10.10.32.57:2181,10.10.32.58:2181,10.10.32.59:2181";
	final String ZK_ADDRESS = "127.0.0.1";
	final String DATABASE_DESC_PREFIX = "/database_desc/";

	/** */
	final String PATTERN = "pattern";
	final String HOST = "host";
	final String PORT = "port";
	final String USER = "user";
	final String PASSWORD = "password";
	final String FLAG = "flag";
	final String DB_NAME = "db_name";

	final String CORE_SIZE = "core_size";
	final String MAX_SIZE = "max_size";

	final String READ_FLAG = "R";
	final String WRITE_FLAG = "W";
	final String RW_FLAG = "RW";

	// logger
	final Logger logger = Logger.getLogger(com.dajie.core.util.DbAccessLogger.class);

}
