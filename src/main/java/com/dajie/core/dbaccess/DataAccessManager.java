package com.dajie.core.dbaccess;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

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

	public <T> List<T> querList(final OpList<T> op) throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			conn = getConnection();
			System.out.println("conn:" + conn.toString());
			ps = conn.prepareStatement(op.getSql());
			System.out.println("ps:" + ps.toString());
			op.setParam(ps);
			rs = ps.executeQuery();
			System.out.println("rs:" + rs.toString());
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

	private Connection getConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String databaseHost = "192.168.9.204";
			String url = "jdbc:mysql://" + databaseHost
					+ ":3309/test?useunicode=true&characterencoding=utf8";
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
