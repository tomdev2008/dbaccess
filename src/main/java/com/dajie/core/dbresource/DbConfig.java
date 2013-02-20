package com.dajie.core.dbresource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.dajie.core.zk.ZNodeListener;
import com.dajie.core.zk.ZkClient;
import com.dajie.core.zk.ZookeeperException;
import com.jolbox.bonecp.BoneCPDataSource;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class DbConfig extends ZNodeListener implements ConnectionAccess {

	private static Logger logger = Constants.logger;

	private String bizName;

	private Random random;
	/** */
	private List<Entry> readEntryList;
	private List<Entry> writeEntryList;
	private Object gate;

	private Set<String> lastChildrenNames = new HashSet<String>();

	/** */

	public DbConfig(String bizName) {
		super(Constants.DATABASE_DESC_PREFIX + bizName);
		this.bizName = bizName;
		random = new Random(System.currentTimeMillis());
		readEntryList = new LinkedList<DbConfig.Entry>();
		writeEntryList = new LinkedList<DbConfig.Entry>();
		gate = new Object();
		init();
		logger.debug("bizName:" + bizName + "\treadEntryList.size():"
				+ readEntryList.size() + "\twriteEntryList.size():"
				+ writeEntryList.size());
	}

	public void init() {
		try {
			List<String> contentList = ZkClient.getInstance().getNodes(
					this.getZNode());
			update(contentList);
			ZkClient.getInstance().addZnodeListener(this);
		} catch (ZookeeperException e) {
			logger.error(e);
		}
	}

	public String getBizName() {

		return bizName;
	}

	public Connection getReadConnection() throws Exception {
		if (readEntryList.isEmpty()) {
			return null;
		}
		synchronized (gate) {
			int readEntrySize = readEntryList.size();
			int index = random.nextInt(readEntrySize);
			Connection conn = null;
			conn = readEntryList.get(index).getConnection();
			return conn;
		}
	}

	public Connection getReadConnection(String pattern) throws Exception {
		if (readEntryList.isEmpty()) {
			return null;
		}
		synchronized (gate) {
			Connection conn = null;
			for (Entry entry : readEntryList) {
				if (entry.match(pattern)) {
					conn = entry.getConnection();
				}
			}
			return conn;
		}
	}

	public Connection getWriteConnection() throws Exception {
		if (writeEntryList.isEmpty()) {
			return null;
		}
		synchronized (gate) {
			Connection conn = null;
			for (Entry entry : writeEntryList) {
				conn = entry.getConnection();
			}
			return conn;
		}
	}

	public Connection getWriteConnection(String pattern) throws Exception {
		if (writeEntryList.isEmpty()) {
			return null;
		}
		synchronized (gate) {
			Connection conn = null;
			for (Entry entry : writeEntryList) {
				if (entry.match(pattern)) {
					conn = entry.getConnection();
				}
			}
			return conn;
		}
	}

	@Override
	public boolean update(List<String> childrenNameList) {

		Set<String> nameSet = new HashSet<String>(childrenNameList);
		if (nameSet.equals(lastChildrenNames)) {
			logger.info("config no change! bizName:" + bizName);
			return true;
		}

		List<Entry> newReadEntryList = new LinkedList<DbConfig.Entry>();
		List<Entry> newWriteEntryList = new LinkedList<DbConfig.Entry>();
		for (String nodeString : childrenNameList) {
			try {
				JSONObject node = new JSONObject(nodeString);
				String host = node.getString(Constants.HOST);
				int port = node.getInt(Constants.PORT);
				String user = node.getString(Constants.USER);
				String password = node.getString(Constants.PASSWORD);
				String flag = node.getString(Constants.FLAG);
				String patternStr = "";
				try {
					patternStr = node.getString(Constants.PATTERN);
				} catch (Exception e) {
					// do not have to log
				}
				String dbName = node.getString(Constants.DB_NAME);
				int coreSize = node.getInt(Constants.CORE_SIZE);
				int maxSize = node.getInt(Constants.MAX_SIZE);
				Entry entry = new Entry(host, port, user, password, flag,
						patternStr, dbName, coreSize, maxSize);
				if (entry.isWritable()) {
					newWriteEntryList.add(entry);
				} else {
					newReadEntryList.add(entry);
				}
			} catch (JSONException e) {
				logger.error(e);
			}
		}
		synchronized (gate) {
			// update set
			lastChildrenNames.clear();
			lastChildrenNames.addAll(nameSet);
			// swap between two list and destroy old list resources

			List<Entry> oldReadList = readEntryList;
			readEntryList = newReadEntryList;
			// destroy old read list
			for (Entry item : oldReadList) {
				try {
					item.closeDataSource();
				} catch (Exception e) {
					logger.error(e);
				}
			}

			List<Entry> oldWriteList = writeEntryList;
			writeEntryList = newWriteEntryList;
			// destroy old read list
			for (Entry item : oldWriteList) {
				try {
					item.closeDataSource();
				} catch (Exception e) {
					logger.error(e);
				}
			}
		}
		return true;
	}

	private static class Entry {
		private String host;
		private int port;
		private String user;
		private String password;
		private String rwFlag;
		private String patternStr;
		private Pattern pattern;
		private String dbName;
		private int coreSize;
		private int maxSize;

		private static final String CONNECTION_URL_PREFIX = "jdbc:mysql://";
		private static final String CONNECTION_URL_SUFFIX = "?useunicode=true&characterencoding=utf8";
		private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";

		private DataSource dataSource;

		public Entry(String host, int port, String user, String password,
				String rwFlag, String patternStr, String dbName, int coreSize,
				int maxSize) {
			this.host = host;
			this.port = port;
			this.user = user;
			this.password = password;
			this.rwFlag = rwFlag;
			this.patternStr = patternStr;
			if (this.patternStr != null && !this.patternStr.isEmpty()) {
				this.pattern = Pattern.compile(this.patternStr);
			} else {
				this.pattern = Pattern.compile("bad bad bad regex string");
			}
			this.dbName = dbName;
			this.coreSize = coreSize;
			this.maxSize = maxSize;
			this.dataSource = null;
			initDataSource();
		}

		private void initDataSource() {
			BoneCPDataSource ds = new BoneCPDataSource();
			ds.setDriverClass(MYSQL_DRIVER_CLASS);
			ds.setUsername(user);
			ds.setPassword(password);
			ds.setJdbcUrl(getConnectionUrl());
			ds.setMinConnectionsPerPartition(coreSize);
			ds.setMaxConnectionsPerPartition(maxSize);
			ds.setConnectionTimeoutInMs(1000);
			ds.setAcquireIncrement(2);
			ds.setIdleConnectionTestPeriodInSeconds(10);
			ds.setIdleMaxAgeInSeconds(10);
			ds.setConnectionTestStatement("SELECT 1");
			ds.setReleaseHelperThreads(0);
			ds.setLogStatementsEnabled(false);
			ds.setAcquireRetryDelayInMs(1000);
			ds.setAcquireRetryAttempts(3);
			ds.setLazyInit(false);
			ds.setDisableJMX(true);
			ds.setPoolAvailabilityThreshold(10);
			this.dataSource = ds;
		}

		private String getConnectionUrl() {
			StringBuffer sb = new StringBuffer();
			sb.append(CONNECTION_URL_PREFIX).append(this.host).append(":")
					.append(this.port).append("/").append(this.dbName)
					.append(CONNECTION_URL_SUFFIX);
			return sb.toString();
		}

		public boolean isWritable() {
			return rwFlag != null && rwFlag.contains(Constants.WRITE_FLAG);
		}

		public boolean isReadable() {
			return rwFlag != null && rwFlag.contains(Constants.READ_FLAG);
		}

		public Connection getConnection() throws SQLException {
			return dataSource.getConnection();
		}

		public void closeDataSource() throws SQLException {
			if (dataSource != null) {
				BoneCPDataSource ds = (BoneCPDataSource) dataSource;
				ds.close();
			}
		}

		public boolean match(String pat) {
			if (pat == null) {
				return false;
			}
			return pattern.matcher(pat).matches();
		}

		@Override
		public String toString() {
			JSONObject json = new JSONObject();
			try {
				json.put("host", host);
				json.put("port", port);
				json.put("user", user);
				json.put("password", "******");
				json.put("flag", rwFlag);
				json.put("pattern", pattern);
				json.put("db_name", dbName);
				json.put("coreSize", coreSize);
				json.put("maxSize", maxSize);
				json.put("dataSource", dataSource.toString());
			} catch (JSONException e) {
				logger.error(e);
			}
			return json.toString();
		}
	}
}
