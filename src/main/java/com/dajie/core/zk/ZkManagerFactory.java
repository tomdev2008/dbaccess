package com.dajie.core.zk;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class ZkManagerFactory {
	private static Log logger = LogFactory.getLog(ZkManagerFactory.class);
	private static ConcurrentHashMap<String, ZkManager> zkManagerMap = new ConcurrentHashMap<String, ZkManager>();

	public static synchronized ZkManager get(String zkAddress) {
		logger.info("ZkManagerFactory.get zkAddress " + zkAddress);
		ZkManager zm = zkManagerMap.get(zkAddress);
		if (null == zm) {
			logger.info("ZkManagerFactory.get new ZkManager for zkAddress "
					+ zkAddress);
			zm = new ZkManager();
			try {
				if (zm.initialize(zkAddress)) {
					zkManagerMap.put(zkAddress, zm);
				} else {
					return null;
				}
			} catch (IOException e) {
				logger.error("ZkManagerFactory.get IOException " + e
						+ " for zkAddress " + zkAddress);
			}
		}
		return zm;
	}
}