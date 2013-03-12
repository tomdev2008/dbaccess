package com.dajie.core.dbaccess;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.dajie.core.dbresource.Constants;
import com.dajie.core.dbresource.DbConfig;
import com.dajie.core.dbresource.DbConfigManager;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class DataAccessManager {

    private static Logger logger = Constants.logger;

    private static DataAccessManager instance;

    private DataAccessManager() {

    }

    public static DataAccessManager getInstance() {
        if (instance == null) {
            synchronized (DataAccessManager.class) {
                instance = new DataAccessManager();
            }
        }
        return instance;
    }

    private void closeResultSet(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void closeStatement(Statement st) {
        try {
            if (st != null) {
                st.close();
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    private void closeConnection(Connection conn) {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public <T> List<T> queryList(final OpList<T> op) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getReadConnection(op);
            logger.debug("queryList(1), conn:" + conn.getMetaData().getURL());
            ps = conn.prepareStatement(op.getSql());
            op.setParam(ps);
            rs = ps.executeQuery();
            while (rs.next()) {
                op.add(op.parse(rs));
            }
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
            closeConnection(conn);
        }
        return op.getResult();
    }

    public <T> List<T> queryList(final OpListR<T> op, Class<T> cla) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getReadConnection(op);
            logger.debug("queryList(2), conn:" + conn.getMetaData().getURL());
            ps = conn.prepareStatement(op.getSql());
            op.setParam(ps);
            rs = ps.executeQuery();
            while (rs.next()) {
                op.add(op.parse(rs, cla));
            }
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
            conn = getReadConnection(op);
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

    public <T> T queryUnique(final OpUniqueR<T> op, Class<T> cla) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getReadConnection(op);
            logger.debug("queryUnique(2), conn:" + conn.getMetaData().getURL());
            ps = conn.prepareStatement(op.getSql());
            op.setParam(ps);
            rs = ps.executeQuery();
            if (rs.next()) {
                T obj = op.parse(rs, cla);
                op.setResult(obj);
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
            conn = getReadConnection(op);
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

    public boolean update(final OpUpdate op) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getWriteConnection(op);
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

    public int insertAndReturnId(final OpUpdate op) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            conn = getWriteConnection(op);
            logger.debug("insertAndReturnId(), conn:" + conn.getMetaData().getURL());
            ps = conn.prepareStatement(op.getSql());
            op.setParam(ps);
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

    private <T> Connection getWriteConnection(Operation<T> op) throws Exception {
        DbConfig dbConfig = DbConfigManager.getInstance().getConfig(op.getBizName());
        if (dbConfig == null) {
            throw new NotAvailableConnectionException("biz:" + op.getBizName() + "\tpattern:"
                    + op.getPattern());
        }
        Connection conn = null;
        if (op.isRouter()) {
            conn = dbConfig.getWriteConnection(op.getPattern());
        } else {
            conn = dbConfig.getWriteConnection();
        }
        if (conn == null) {
            throw new NotAvailableConnectionException("biz:" + op.getBizName() + "\tpattern:"
                    + op.getPattern() + "\tflag:W");
        }
        return conn;
    }

    private <T> Connection getReadConnection(Operation<T> op) throws Exception {
        DbConfig dbConfig = DbConfigManager.getInstance().getConfig(op.getBizName());
        if (dbConfig == null) {
            throw new NotAvailableConnectionException("biz:" + op.getBizName() + "\tpattern:"
                    + op.getPattern());
        }
        Connection conn = null;
        if (op.isRouter()) {
            conn = dbConfig.getReadConnection(op.getPattern());
        } else {
            conn = dbConfig.getReadConnection();
        }
        if (conn == null) {
            throw new NotAvailableConnectionException("biz:" + op.getBizName() + "\tpattern:"
                    + op.getPattern() + "\tflag:R");
        }
        return conn;
    }

    private <T> Connection getReadConnection(OperationR<T> op) throws Exception {
        DbConfig dbConfig = DbConfigManager.getInstance().getConfig(op.getBizName());
        if (dbConfig == null) {
            throw new NotAvailableConnectionException("biz:" + op.getBizName() + "\tpattern:"
                    + op.getPattern());
        }
        Connection conn = null;
        if (op.isRouter()) {
            conn = dbConfig.getReadConnection(op.getPattern());
        } else {
            conn = dbConfig.getReadConnection();
        }
        if (conn == null) {
            throw new NotAvailableConnectionException("biz:" + op.getBizName() + "\tpattern:"
                    + op.getPattern() + "\tflag:R");
        }
        return conn;
    }
}
