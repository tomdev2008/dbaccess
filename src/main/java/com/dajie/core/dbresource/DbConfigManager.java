package com.dajie.core.dbresource;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class DbConfigManager {

	// bizName to DbConfig map
	private Map<String, DbConfig> dbConfigMap;

	private static DbConfigManager instance;

	private DbConfigManager() {
		dbConfigMap = new HashMap<String, DbConfig>();
	}

	public static DbConfigManager getInstance() {
		if (instance == null)
			synchronized (DbConfigManager.class) {
				instance = new DbConfigManager();
			}
		return instance;
	}

	public DbConfig getConfig(String bizName) throws IllegalArgumentException {
		if ("".equals(bizName)) {
			throw new IllegalArgumentException("argv:" + bizName);
		}
		synchronized (DbConfigManager.class) {
			DbConfig config = dbConfigMap.get(bizName);
			if (config == null) {
				config = new DbConfig(bizName);
				dbConfigMap.put(bizName, config);
			}
			return dbConfigMap.get(bizName);
		}
	}
}
