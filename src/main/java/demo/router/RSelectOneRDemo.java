package demo.router;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.BasicConfigurator;

import sirius.dbaccess.DataAccessManager;
import sirius.dbaccess.OpUniqueR;
import sirius.dbresource.Keys;
import demo.Geek;

public class RSelectOneRDemo {

    static void foo() {

        final int id = 1;
        int mod = 10;
        int suffix = id % mod;
        String tableName = "geek_" + suffix;
        String sql = "select ID,NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE FROM " + tableName
                + " WHERE ID = ?";

        OpUniqueR<Geek> op = new OpUniqueR<Geek>(sql, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, id);
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
