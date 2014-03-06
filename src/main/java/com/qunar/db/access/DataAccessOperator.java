package com.qunar.db.access;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qunar.db.resource.ReadOnlyDataSource;
import com.qunar.db.resource.ReadWriteDataSource;
import com.qunar.db.util.DbLogger;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class DataAccessOperator {

    private static final Logger logger = LoggerFactory.getLogger(DbLogger.class);

    private String namespace;

    private String cipher;

    private ReadOnlyDataSource readOnlyDataSource;

    private ReadWriteDataSource readWriteDataSource;

    public DataAccessOperator(String namespace, String cipher) {
        this.namespace = namespace;
        this.cipher = cipher;
        this.readOnlyDataSource = new ReadOnlyDataSource(this.namespace, this.cipher);
        this.readWriteDataSource = new ReadWriteDataSource(this.namespace, this.cipher);
    }

    protected String desc(String msg) {
        StringBuffer sb = new StringBuffer();
        sb.append("[ns:").append(namespace);
        if (msg != null) {
            sb.append(", msg:").append(msg);
        }
        sb.append("]");
        return sb.toString();
    }

    private void closeResultSet(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            logger.error(desc(e.getMessage()), e);
        }
    }

    private void closeStatement(Statement st) {
        try {
            if (st != null) {
                st.close();
            }
        } catch (Exception e) {
            logger.error(desc(e.getMessage()), e);
        }
    }

    private void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            logger.error(desc(e.getMessage()), e);
        }
    }

    private Connection getReadConnection() throws Exception {
        Connection conn = readOnlyDataSource.getConnection();
        if (conn == null) {
            conn = readWriteDataSource.getConnection();
        }
        if (conn == null) {
            throw new NotAvailableConnectionException(desc("Not Avaliable Connection!"));
        }
        return conn;
    }

    private Connection getWriteConnection() throws Exception {
        Connection conn = readWriteDataSource.getConnection();
        if (conn == null) {
            throw new NotAvailableConnectionException(desc("Not Avaliable Connection!"));
        }
        return conn;
    }

    public <T> List<T> queryList(final OpList<T> op) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getReadConnection();
            logger.debug("queryList(1), conn:" + conn.getMetaData().getURL());
            ps = conn.prepareStatement(op.getSql());
            op.setParam(ps);
            rs = ps.executeQuery();
            while (rs.next()) {
                op.add(op.parse(rs));
            }
        } catch (Exception e) {
            logger.error(desc(e.getMessage()), e);
            throw e;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(conn);
        }
        return op.getResult();
    }

    public <T> T queryUnique(final OpUnique<T> op) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getReadConnection();
            logger.debug("queryUnique(1), conn:" + conn.getMetaData().getURL());
            ps = conn.prepareStatement(op.getSql());
            op.setParam(ps);
            rs = ps.executeQuery();
            if (rs.next()) {
                T result = op.parse(rs);
                op.setResult(result);
            }

        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(conn);
        }
        return op.getResult();
    }

    public <K, T> Map<K, T> queryMap(final OpMap<K, T> op) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getReadConnection();
            logger.debug("queryMap(), conn:" + conn.getMetaData().getURL());
            ps = conn.prepareStatement(op.getSql());
            op.setParam(ps);
            rs = ps.executeQuery();
            while (rs.next()) {
                op.parse(rs);
            }
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(conn);
        }
        return op.getResult();
    }

    public int insertAndReturnId(final OpUpdate op) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getWriteConnection();
            logger.debug("insertAndReturnId(), conn:" + conn.getMetaData().getURL());
            ps = conn.prepareStatement(op.getSql());
            op.setParam(ps);
            logger.debug("sql:" + op.getSql());
            int rows = ps.executeUpdate();
            if (rows > 0) {
                if (ps != null) {
                    ps.close();
                }
                ps = conn.prepareStatement("SELECT LAST_INSERT_ID();");
                rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }

        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(conn);
        }
        return -1;
    }

    public boolean update(final OpUpdate op) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getWriteConnection();
            ps = conn.prepareStatement(op.getSql());
            logger.debug("update(), conn:" + conn.getMetaData().getURL());
            op.setParam(ps);
            int rows = ps.executeUpdate();
            op.setResult(rows);
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(conn);
        }
        return (op.getResult() > 0 ? true : false);
    }
}
