package demo;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.BasicConfigurator;

import sirius.dbaccess.DataAccessManager;
import sirius.dbaccess.OpUniqueR;
import sirius.dbresource.Keys;


public class SelectOneRDemo {

    static void foo() {
        String sql = "select ID,NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE FROM geek WHERE ID = ?";

        OpUniqueR<Geek> op = new OpUniqueR<Geek>(sql, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, 3);
            }
        };

        Geek g = null;
        try {
            g = DataAccessManager.getInstance().queryUnique(op, Geek.class);
            System.out.println(g);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        foo();
        System.exit(0);
    }

}
