package com.dajie.core.dbaccess;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public abstract class OpUpdate extends Operation<Integer> {

	private int result;

	public OpUpdate(String sql, String bizName) {
		setSql(sql);
		setBizName(bizName);
	}

	public OpUpdate(String sql, String bizName, int tableSuffix) {
		setSql(sql);
		setBizName(bizName);
		setTableSuffix(tableSuffix);
	}

	public int getResult() {
		return result;
	}

	public void setResult(int result) {
		this.result = result;
	}

	/**
	 *  OpUpdate needn't implement parse method
	 */
	@Override
	public Integer parse(ResultSet rs) throws SQLException {
		// empty impl
		return -1;
	}
}
