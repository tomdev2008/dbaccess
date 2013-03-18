package com.dajie.core.dbaccess;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 * @param <T>
 */
public abstract class OpList<T> extends Operation<T> {

    private List<T> collection;

    private void initEmptyCollection() {
        collection = new ArrayList<T>();
    }

    public OpList(String sql, String bizName) {
        setSql(sql);
        setBizName(bizName);
        initEmptyCollection();
    }

    public OpList(String sql, String bizName, int tableSuffix) {
        setSql(sql);
        setBizName(bizName);
        setTableSuffix(tableSuffix);
        initEmptyCollection();
    }

    public final void add(T t) {
        collection.add(t);
    }

    public List<T> getResult() {
        return collection;
    }
}
