package com.dajie.core.dbaccess;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 * @param <T>
 */
public abstract class OpUnique<T> extends Operation<T> {

    private T result;

    public OpUnique(String sql, String bizName) {
        setResult(null);
        setSql(sql);
        setBizName(bizName);
    }

    public OpUnique(String sql, String bizName, int tableSuffix) {
        setResult(null);
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
