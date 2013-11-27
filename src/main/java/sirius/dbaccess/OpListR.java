package sirius.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import sirius.util.DataUtil;


/**
 * OpListR is a java reflect version for OpList
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public abstract class OpListR<T> extends OperationR<T> {

    private List<T> collection;

    private void initEmptyCollection() {
        collection = new ArrayList<T>();
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

    public T parse(ResultSet rs, Class<T> cla) throws Exception {
        return DataUtil.convert(rs, cla);
    }

    public final void add(T t) {
        collection.add(t);
    }

    public List<T> getResult() {
        return collection;
    }

}
