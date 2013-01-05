package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
}
