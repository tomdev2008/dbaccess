package demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLConnectionDemo {

    //	static final String CONN_STR = "jdbc:mysql://localhost:3306/test?useunicode=true&characterencoding=utf8&autoReconnect=true";
    static final String CONN_STR = "jdbc:mysql://localhost:3306/test?useunicode=true&characterencoding=utf8&autoReconnect=true";

    public static void main(String[] args) {
        init();
        //		save();
        //        get();
    }

    static class Goo {

        private int id;

        private int name;

        public Goo(int id, int name) {
            super();
            this.id = id;
            this.name = name;
        }

    }

    static void init() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    static void save() {

        try {
            Connection conn = DriverManager.getConnection(CONN_STR, "root", "1234");
            Statement stmt = conn.createStatement();
            String name = "a李";
            String sql = "INSERT INTO DB_FOO(id, name) values(100, '" + name + "')";
            System.out.println(sql);
            boolean rs = stmt.execute(sql);
            System.out.println("save:" + rs);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    static void get() {
        try {

            Connection conn = DriverManager.getConnection(CONN_STR, "root", "1234");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select id, name from abc limit 1000");
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(2));
                byte[] data = rs.getBytes(2);
                System.out.println("data.length:" + data.length);
            }
            rs.close();
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
