package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.dajie.core.util.DataUtil;

/**
 * OpUniqueR is java reflect version for OpUnique
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public abstract class OpUniqueR extends OperationR {

	private Object result;

	public OpUniqueR(String sql, String bizName) {

		setSql(sql);
		setBizName(bizName);
	}

	public OpUniqueR(String sql, String bizName, int tableSuffix) {
		setSql(sql);
		setBizName(bizName);
		setTableSuffix(tableSuffix);
	}

	public abstract void setParam(PreparedStatement ps) throws SQLException;

	@Override
	public Object parse(ResultSet rs, Class<? extends Object> cla)
			throws Exception {
		return DataUtil.convert(rs, cla);
	}

	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}

}
