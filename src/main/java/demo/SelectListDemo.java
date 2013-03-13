package demo;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.BasicConfigurator;

import com.dajie.core.dbaccess.DataAccessManager;
import com.dajie.core.dbaccess.OpList;
import com.dajie.core.dbresource.Keys;

public class SelectListDemo {

    static void foo() {
        String sql = "select ID,NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE FROM geek limit 1000";
        OpList<Geek> op = new OpList<Geek>(sql, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {

            }

            @Override
            public Geek parse(ResultSet rs) throws SQLException {
                Geek g = new Geek();
                g.setId(rs.getInt("ID"));
                g.setName(rs.getString("NAME"));
                g.setStatus(rs.getInt("STATUS"));
                g.setHeight(rs.getDouble("HEIGHT"));
                g.setWeight(rs.getFloat("WEIGHT"));
                g.setBirth(rs.getDate("BIRTH"));
                g.setLastLogin(rs.getTimestamp("LAST_LOGIN"));
                g.setScore(rs.getLong("SCORE"));
                return g;
            }
        };

        try {
            List<Geek> geeks = DataAccessManager.getInstance().queryList(op);
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
