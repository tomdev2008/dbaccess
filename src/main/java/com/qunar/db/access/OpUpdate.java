package com.qunar.db.access;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 
 * @author liyong19861014@gmail.com
 */
public abstract class OpUpdate extends Operation<Integer> {

    private int result;

    public OpUpdate(String sql) {
        this(sql, "", -1);
    }

    public OpUpdate(String sql, String bizName) {
        this(sql, bizName, -1);
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
     * OpUpdate needn't implement parse method
     */
    @Override
    public Integer parse(ResultSet rs) throws SQLException {
        // empty impl
        return -1;
    }
}
