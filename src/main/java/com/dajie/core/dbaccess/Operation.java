package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 
 * @author yong.li@dajie-inc.com
 *
 * @param <T>
 */
public abstract class Operation<T> {

    private String bizName;


    private int tableSuffix = -1;

    private String sql;
    
    public boolean isRouter() {
    	return tableSuffix > -1;
    }

    public String getBizName() {
        return bizName;
    }

    public void setBizName(String bizName) {
        this.bizName = bizName;
    }

    public int getTableSuffix() {
        return tableSuffix;
    }

    public void setTableSuffix(int tableSuffix) {
        this.tableSuffix = tableSuffix;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
    
    public String getPattern() {
    	StringBuffer sb = new StringBuffer();
    	sb.append(bizName);
    	if (tableSuffix >= 0) {
    		sb.append("_").append(tableSuffix);
    	}
    	return sb.toString();
    }
    
    public abstract void setParam(PreparedStatement ps) throws SQLException;
    public abstract T parse(ResultSet rs) throws SQLException;
}
