package com.dajie.core.dbaccess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class DataAccessManager {

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
			e.printStackTrace();
		}
	}

	private void closeStatement(Statement st) {
		try {
			if (st != null) {
				st.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void closeConnection(Connection conn) {
		try {
			if (conn != null) {
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public <T> List<T> queryList(final OpList<T> op) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
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

	public <T> T queryUnique(final OpUnique<T> op) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
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

	public <K, T> Map<K, T> queryMap(final OpMap<K, T> op) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
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

	public boolean update(final OpUpdate op) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(op.getSql());
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

	public int insertAndReturnId(final OpUpdate op) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
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

	private Connection getConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String databaseHost = "localhost";
			String url = "jdbc:mysql://" + databaseHost
					+ ":3306/test?useunicode=true&characterencoding=utf8"; //
			String user = "root";
			String password = "12345";
			return DriverManager.getConnection(url, user, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
