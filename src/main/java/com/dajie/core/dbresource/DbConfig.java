package com.dajie.core.dbresource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.json.JSONException;
import org.json.JSONObject;

import com.dajie.core.zk.ZNodeListener;
import com.dajie.core.zk.ZkClient;
import com.dajie.core.zk.ZookeeperException;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class DbConfig extends ZNodeListener implements ConnectionAccess {

	private String bizName;

	private List<Entry> entryList;

	private Object gate;

	public DbConfig(String bizName) {
		super(Constants.DATABASE_DESC_PREFIX + bizName);
		this.bizName = bizName;
		gate = new Object();
		this.entryList = new LinkedList<DbConfig.Entry>();
		init();
	}

	public void init() {
		try {
			List<String> contentList = ZkClient.getInstance().getNodes(
					this.getZNode());
			update(contentList);
			ZkClient.getInstance().addZnodeListener(this);
		} catch (ZookeeperException e) {
			e.printStackTrace();
		}
	}

	public String getBizName() {

		return bizName;
	}

	public Connection getReadConnection() throws Exception {
		synchronized (gate) {
			Connection conn = null;
			for (Entry entry : entryList) {
				if (entry.isReadable()) {
					conn = entry.getConnection();
				}
			}
			return conn;
		}
	}

	public Connection getReadConnection(String pattern) throws Exception {
		synchronized (gate) {
			Connection conn = null;
			for (Entry entry : entryList) {
				if (entry.isReadable()) {
					if (entry.match(pattern)) {
						conn = entry.getConnection();
					}
				}
			}
			return conn;
		}
	}

	public Connection getWriteConnection() throws Exception {
		synchronized (gate) {
			Connection conn = null;
			for (Entry entry : entryList) {
				if (entry.isWritable()) {
					conn = entry.getConnection();
				}
			}
			return conn;
		}
	}

	public Connection getWriteConnection(String pattern) throws Exception {
		synchronized (gate) {
			Connection conn = null;
			for (Entry entry : entryList) {
				if (entry.isWritable()) {
					if (entry.match(pattern)) {
						conn = entry.getConnection();
					}
				}
			}
			return conn;
		}
	}

	@Override
	public boolean update(List<String> childrenNameList) {

		List<Entry> newEntryList = new LinkedList<DbConfig.Entry>();
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
				newEntryList.add(entry);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		synchronized (gate) {
			// swap between two list and destroy old list resources
			List<Entry> oldList = entryList;
			entryList = newEntryList;
			// destroy old list resources
			if (oldList != null) {
				for (Entry item : oldList) {
					try {
						item.closeDataSource();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		return true;
	}

	// public static void main(String[] args) throws Exception {
	// Entry entry = new Entry("localhost", 3306, "root", "12345", "R", null,
	// "test", 5, 10);
	// System.exit(0);
	// }

	static class Entry {
		private String host;
		private int port;
		private String user;
		private String password;
		private String rwFlag; // R=1 or W=2
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
			BasicDataSource ds = new BasicDataSource();
			ds.setDriverClassName(MYSQL_DRIVER_CLASS);
			ds.setUsername(this.user);
			ds.setPassword(this.password);
			ds.setUrl(getConnectionUrl());
			ds.setInitialSize(this.coreSize);
			ds.setMaxActive(maxSize);
			ds.setMaxIdle(coreSize);
			ds.setMaxWait(1000L);
			ds.addConnectionProperty("connectTimeout", "1000");
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

		public boolean isRW() {
			return rwFlag != null && rwFlag.contains(Constants.WRITE_FLAG);
		}

		public Connection getConnection() throws SQLException {
			return dataSource.getConnection();
		}

		public void closeDataSource() throws SQLException {
			if (dataSource != null) {
				BasicDataSource ds = (BasicDataSource) dataSource;
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
				e.printStackTrace();
			}
			return json.toString();
		}
	}
}
