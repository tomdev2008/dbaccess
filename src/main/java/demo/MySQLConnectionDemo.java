package demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLConnectionDemo {

	public static void main(String[] args) {
		
//		String conStr = "jdbc:mysql://localhost:3306/test?useunicode=true&characterencoding=utf8&autoReconnect=true";
		String conStr = "jdbc:mysql://localhost:3306/?useunicode=true&characterencoding=utf8&autoReconnect=true";
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(conStr, "root", "1234");
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("show databases");
			while (rs.next()) {
				System.out.println(rs.getString(1));
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
