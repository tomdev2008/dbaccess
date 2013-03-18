package demo;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.log4j.BasicConfigurator;

import com.dajie.core.dbaccess.DataAccessManager;
import com.dajie.core.dbaccess.OpUpdate;
import com.dajie.core.dbresource.Keys;

public class InsertDemo {

    static void foo() {
        String sql = "INSERT INTO geek(NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE) "
                + "VALUES(?, ?, ?, ?, ?, ?, ?)";
        OpUpdate op = new OpUpdate(sql, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setString(1, "Steve");
                ps.setInt(2, 3);
                ps.setDouble(3, 56.98d);
                ps.setFloat(4, 165.15f);
                Calendar cal = Calendar.getInstance();
                cal.set(1989, 4, 2);
                ps.setDate(5, new Date(cal.getTimeInMillis()));
                cal.set(2013, 1, 2, 11, 35, 51);
                ps.setTimestamp(6, new Timestamp(cal.getTimeInMillis()));
                ps.setLong(7, 102349893482329l);

            }
        };

        try {
            int id = DataAccessManager.getInstance().insertAndReturnId(op);
            System.out.println("insert id:" + id);
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
