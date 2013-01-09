package com.dajie.core.dbresource;

import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.dajie.core.zk.ZNodeListener;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class DatabaseConfig extends ZNodeListener implements DatabaseResource {

	private String bizName;

	private List<Entry> entryList;

	public DatabaseConfig(String znode) {
		super(Constants.DATABASE_DESC_PREFIX + znode);
		this.bizName = znode;
		this.entryList = new LinkedList<DatabaseConfig.Entry>();
	}

	@Override
	public boolean update(List<String> childrenNameList) {
		for (String nodeString : childrenNameList) {
			try {
				JSONObject node = new JSONObject(nodeString);
				String host = node.getString(Constants.HOST);
				int port = node.getInt(Constants.PORT);
				String user = node.getString(Constants.USER);
				String password = node.getString(Constants.PASSWORD);
				String flag = node.getString(Constants.FLAG);
				String pattern = node.getString(Constants.PATTERN);
				int coreSize = node.getInt(Constants.CORE_SIZE);
				int maxSize = node.getInt(Constants.MAX_SIZE);
				Entry entry = new Entry(host, port, user, password, flag,
						pattern, coreSize, maxSize);
				entryList.add(entry);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	public Connection getReadConnection(String pattern) {
		return null;
	}

	public Connection getWriteConnection(String pattern) {
		return null;
	}

	public static void main(String[] args) {

		System.exit(0);
	}

	static class Entry {
		private String host;
		private int port;
		private String user;
		private String password;
		private String rwFlag; // R=1 or W=2, disabled=0
		private String pattern;
		private int coreSize;
		private int maxSize;

		// TODO: private DataSource dataSource;

		public Entry(String host, int port, String user, String password,
				String rwFlag, String pattern, int coreSize, int maxSize) {
			this.host = host;
			this.port = port;
			this.user = user;
			this.password = password;
			this.rwFlag = rwFlag;
			this.pattern = pattern;
			this.coreSize = coreSize;
			this.maxSize = maxSize;
		}

		public boolean isWriteEntry() {
			return rwFlag != null && rwFlag.contains(Constants.WRITE_FLAG);
		}

		public boolean isReadEntry() {
			return rwFlag != null && rwFlag.contains(Constants.READ_FLAG);
		}

		public boolean isReadWriteEntry() {
			return rwFlag != null && rwFlag.contains(Constants.WRITE_FLAG);
		}
	}

}
