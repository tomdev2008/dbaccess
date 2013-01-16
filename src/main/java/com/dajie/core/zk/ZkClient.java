package com.dajie.core.zk;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dajie.core.dbresource.Constants;


/**
 * 
 * @author yong.li@dajie-inc.com
 *
 */
public class ZkClient {

	private static Log logger = LogFactory.getLog(ZkClient.class);

	static final String UPDATE_ALL_NAMESPACE = "HOLY_SHIT_UPDATE_ALL_NAMESPACE";

	private static Map<String, ZkClient> zkClientMap = new HashMap<String, ZkClient>();

	private static Object zkClientMapGate = new Object();

	//
	private String zkAddress;

	private ZkManager zkManager;

	private boolean valid;

	private Map<String, List<ZNodeListener>> listeners;

	private ReentrantReadWriteLock listenerRWLock;

//	private Map<String, String> znodeVersionMap;
//
//	private Object znodeVersionLock;
	
	private ZkClient(String zkAddress) throws ZookeeperException {
		this.zkAddress = zkAddress;
		initialize();
	}
	
	public static ZkClient getInstance() throws ZookeeperException {
		return getInstance(Constants.ZK_ADDRESS);
	}

	public static ZkClient getInstance(String zkAddress)
			throws ZookeeperException {

		String newZkAddress = null;
		try {
			newZkAddress = System.getProperty("ZK_ADDRESS");
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (newZkAddress == null) {
			try {
				newZkAddress = System.getenv("ZK_ADDRESS");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (newZkAddress == null) {
			newZkAddress = zkAddress;
		}

		ZkClient zkClient = null;
		synchronized (zkClientMapGate) {
			zkClient = zkClientMap.get(newZkAddress);
			if (zkClient != null) {
				return zkClient;
			} else {
				try {
					zkClient = new ZkClient(newZkAddress);
					zkClientMap.put(newZkAddress, zkClient);
				} catch (Exception e) {
					logger.error("Zkclient.getInstance(String) zkAddress:"
							+ newZkAddress);
				}
			}
		}
		return zkClient;
	}

	private boolean initialize() throws ZookeeperException {
		zkManager = ZkManagerFactory.get(zkAddress);
		valid = zkManager != null ? true : false;
		if (!isValid()) {
			throw new ZookeeperException(
					"ZkManagerFactory.get return null\t zkAddress:" + zkAddress);
		}
		zkManager.setZkClient(this);
		listeners = new HashMap<String, List<ZNodeListener>>();
		listenerRWLock = new ReentrantReadWriteLock();
//		znodeVersionMap = new HashMap<String, String>();
//		znodeVersionLock = new Object();
		return isValid();
	}

	public boolean isValid() {
		return valid == true;
	}

	/**
	 * 被外面回调的更新方法
	 * 
	 * @param znodePath
	 * @return
	 */
	public boolean update(String znodePath) {
		if (UPDATE_ALL_NAMESPACE.equals(znodePath)) {
			return updateAllZNodes();
		}
		boolean flag = updateZNode(znodePath);
		logger.info("ZkClient.update() flag:" + flag);
		return flag;
	}

	/**
	 * 取出namespace下,node name list
	 * 
	 * @param namespace
	 * @return
	 */
	public List<String> getNodes(String znodePath) {
		if ("".equals(znodePath)) {
			return null;
		}
		return zkManager.getChildren(znodePath);
	}

	/**
	 * 增加namespace listener
	 * 
	 * @param listener
	 */
	public void addZnodeListener(ZNodeListener listener) {
		try {
			listenerRWLock.writeLock().lock();
			List<ZNodeListener> listenerList = listeners.get(listener
					.getZNode());
			if (listenerList != null) {
				listenerList.add(listener);
			} else {
				listenerList = new ArrayList<ZNodeListener>();
				listenerList.add(listener);
				listeners.put(listener.getZNode(), listenerList);
			}
		} finally {
			listenerRWLock.writeLock().unlock();
		}
	}

	/**
	 * when holy shit happens!!
	 * 
	 * @return
	 */
	private boolean updateAllZNodes() {
		boolean flag = true;
		try {
			listenerRWLock.readLock().lock();
			for (String znode : listeners.keySet()) {
				boolean result = updateZNode(znode);
				if (!result) {
					flag = false;
				}
			}
		} finally {
			listenerRWLock.readLock().unlock();
		}
		logger.info("ZkClient.updateAllNamespaces() flag:" + flag);
		return flag;
	}

	private boolean updateZNode(String znodePath) {
		boolean flag = true;
//		String newVersion = getZNodeVersion(znodePath);
//		synchronized (znodeVersionLock) {
//			String oldVersion = znodeVersionMap.get(znodePath);
//			if (newVersion.equals(oldVersion)) {
//				return true;
//			}
//			znodeVersionMap.put(znodePath, newVersion);
//		}
		List<String> childrenNames = zkManager.getChildren(znodePath);
		try {
			listenerRWLock.readLock().lock();
			List<ZNodeListener> listenerList = listeners.get(znodePath);
			if (listenerList == null) {
				logger.info("znode:" + znodePath + " listenerList is null.");
				return true;
			}
			for (ZNodeListener listener : listenerList) {
				boolean ret = listener.update(childrenNames);
				if (!ret) {
					flag = ret;
				}
			}
		} finally {
			listenerRWLock.readLock().unlock();
		}
		return flag;
	}

	// private String getZNodeVersion(String znodePath) {
	// return zkManager.getData(znodePath);
	// }
}
