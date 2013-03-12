package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.dajie.core.util.DataUtil;

/**
 * OpListR is a java reflect version for OpList
 * @author yong.li@dajie-inc.com
 * 
 */
public abstract class OpListR extends OperationR {

	private List<Object> collection;

	private void initEmptyCollection() {
		collection = new ArrayList<Object>();
	}

	public OpListR(String sql, String bizName) {
		setSql(sql);
		setBizName(bizName);
		initEmptyCollection();
	}

	public OpListR(String sql, String bizName, int tableSuffix) {
		setSql(sql);
		setBizName(bizName);
		setTableSuffix(tableSuffix);
		initEmptyCollection();
	}

	public abstract void setParam(PreparedStatement ps) throws SQLException;

	@Override
	public Object parse(ResultSet rs, Class<? extends Object> cla)
			throws Exception {
		return DataUtil.convert(rs, cla);
	}

	public final void add(Object t) {
		collection.add(t);
	}

	public List<Object> getResult() {
		return collection;
	}

}
