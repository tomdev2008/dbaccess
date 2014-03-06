package com.qunar.db.access;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author liyong19861014@gmail.com
 * @param <T>
 */
public abstract class OpList<T> extends Operation<T> {

    private List<T> collection;

    private void initEmptyCollection() {
        collection = new ArrayList<T>();
    }

    public OpList(String sql) {
        this(sql, "", -1);
    }

    public OpList(String sql, String bizName) {
        this(sql, bizName, -1);
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
