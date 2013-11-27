package demo;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.BasicConfigurator;

import sirius.dbaccess.DataAccessManager;
import sirius.dbaccess.OpUpdate;
import sirius.dbresource.Keys;


public class DeleteDemo {

    static void foo() {
        String sql = "DELETE FROM geek WHERE ID = ?";
        OpUpdate op = new OpUpdate(sql, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, 12);
            }
        };

        try {
            boolean result = DataAccessManager.getInstance().update(op);
            System.out.println("result:" + result);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        foo();
        System.exit(0);
    }

}
