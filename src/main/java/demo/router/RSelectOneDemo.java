package demo.router;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.BasicConfigurator;

import com.dajie.core.dbaccess.DataAccessManager;
import com.dajie.core.dbaccess.OpUnique;
import com.dajie.core.dbresource.Keys;

import demo.Geek;

public class RSelectOneDemo {

    static void foo() {

        final int id = 1;
        int mod = 10;
        int suffix = id % mod;
        String tableName = "geek_" + suffix;
        String sql = "select ID,NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE FROM " + tableName
                + " WHERE ID = ?";

        OpUnique<Geek> op = new OpUnique<Geek>(sql, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, id);
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

        Geek g = null;
        try {
            g = DataAccessManager.getInstance().queryUnique(op);
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
