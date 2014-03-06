package com.qunar.db.access;

/**
 * 
 * @author liyong19861014@gmail.com
 * @param <T>
 */
public abstract class OpUnique<T> extends Operation<T> {

    private T result;

    public OpUnique(String sql) {
        this(sql, "", -1);
    }

    public OpUnique(String sql, String bizName) {
        this(sql, bizName, -1);
    }

    public OpUnique(String sql, String bizName, int tableSuffix) {
        setSql(sql);
        setBizName(bizName);
        setTableSuffix(tableSuffix);
    }

    public void setResult(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }
}
