package com.dajie.core.dbaccess;

import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 * @param <K>
 * @param <T>
 */
public abstract class OpMap<K, T> extends Operation<T> {

    private Map<K, T> result;

    private void initEmptyResult() {
        result = new HashMap<K, T>();
    }

    public OpMap(String sql, String bizName) {
        setSql(sql);
        setBizName(bizName);
        initEmptyResult();
    }

    public OpMap(String sql, String bizName, int tableSuffix) {
        setSql(sql);
        setBizName(bizName);
        setTableSuffix(tableSuffix);
        initEmptyResult();
    }

    public Map<K, T> getResult() {
        return result;
    }

    public void setResult(Map<K, T> result) {
        this.result = result;
    }

    public void add(K key, T value) {
        result.put(key, value);
    }

}
