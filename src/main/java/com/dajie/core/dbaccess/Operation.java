package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 
 * @author liyong
 * 
 */
public abstract class Operation<T> {

    private String bizName;

    private String tableName;

    private int tableSuffix = -1;

    private String sql;

    public String getBizName() {
        return bizName;
    }

    public void setBizName(String bizName) {
        this.bizName = bizName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
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
    
    public abstract void setParam(PreparedStatement ps) throws SQLException;
    public abstract T parse(ResultSet rs) throws SQLException;
}
