package demo;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.BasicConfigurator;

import sirius.dbaccess.DataAccessManager;
import sirius.dbaccess.OpListR;
import sirius.dbresource.Keys;


public class SelectListRDemo {

    static void foo() {
        String sql = "select ID,NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE FROM geek limit 1000";
        OpListR<Geek> op = new OpListR<Geek>(sql, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
            }
        };
        try {
            List<Geek> list = DataAccessManager.getInstance().queryList(op, Geek.class);
            for (Geek g : list) {
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
