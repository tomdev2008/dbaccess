package demo.router;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.log4j.BasicConfigurator;

import sirius.dbaccess.DataAccessManager;
import sirius.dbaccess.OpUpdate;
import sirius.dbresource.Keys;


public class RInsertDemo {

    static void foo() {

        for (int i = 21; i < 100; ++i) {
            final int id = i;
            int mod = 10;
            int suffix = id % mod;
            String tableName = "geek_" + suffix;
            String sql = "INSERT INTO "
                    + tableName
                    + "(ID,NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
            OpUpdate op = new OpUpdate(sql, Keys.GEEK_ROUTER, suffix) {

                @Override
                public void setParam(PreparedStatement ps) throws SQLException {
                    ps.setInt(1, id);
                    ps.setString(2, "unknown_name_" + id);
                    ps.setInt(3, 3);
                    ps.setDouble(4, 56.98d);
                    ps.setFloat(5, 165.15f);
                    Calendar cal = Calendar.getInstance();
                    cal.set(1989, 4, 2);
                    ps.setDate(6, new Date(cal.getTimeInMillis()));
                    cal.set(2013, 1, 2, 11, 35, 51);
                    ps.setTimestamp(7, new Timestamp(cal.getTimeInMillis()));
                    ps.setLong(8, 102349893482329l);

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

    }

    public static void main(String[] args) {
        BasicConfigurator.configure();
        foo();
        System.exit(0);
    }

}
