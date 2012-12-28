package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;



public abstract class OpList<T> extends Operation<T> {
    
    private List<T> collection;
    
    public OpList(String sql, String bizName) {
        setSql(sql);
        setBizName(bizName);
        collection = new ArrayList<T>();
    }
    
    public OpList(String sql, String bizName, int tableSuffix) {
        setSql(sql);
        setBizName(bizName);
        setTableSuffix(tableSuffix);
    }
    
    public final void add(T t) {
        collection.add(t);
    }
    
    public List<T> getResult() {
        return collection;
    }

    
    public static void main(String[] args) {
        OpList<Person> list = new OpList<Person>("bc", "ff") {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                // TODO Auto-generated method stub
                
            }

            @Override
            public Person parse(ResultSet rs) throws SQLException {
                // TODO Auto-generated method stub
                return null;
            }
            
        };
    }

}
