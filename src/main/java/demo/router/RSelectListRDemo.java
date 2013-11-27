package demo.router;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.BasicConfigurator;

import sirius.dbaccess.DataAccessManager;
import sirius.dbaccess.OpListR;
import sirius.dbresource.Keys;


import demo.Geek;

public class RSelectListRDemo {

    static void foo() {

        int suffix = 3;
        String tableName = "geek_" + suffix;
        String sql = "select ID,NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE FROM " + tableName
                + " LIMIT ?";
        OpListR<Geek> op = new OpListR<Geek>(sql, Keys.GEEK_ROUTER, suffix) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, 100);
            }
        };

        try {
            List<Geek> geeks = DataAccessManager.getInstance().queryList(op, Geek.class);
            for (Geek g : geeks) {
                System.out.println(g);
            }
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
