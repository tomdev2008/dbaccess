package demo.dao.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.dajie.core.dbaccess.DataAccessManager;
import com.dajie.core.dbaccess.OpUniqueR;
import com.dajie.core.dbaccess.OpUpdate;
import com.dajie.core.dbresource.Keys;

import demo.Geek;
import demo.dao.GeekDAO;

public class GeekDAOImpl implements GeekDAO {

    private static final String SELECT_BY_ID_SQL = "select ID,NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE FROM geek WHERE ID = ?";

    private static final String INSERT_SQL = "INSERT INTO geek(NAME,STATUS,HEIGHT,WEIGHT,BIRTH,LAST_LOGIN,SCORE) VALUES(?, ?, ?, ?, ?, ?, ?)";

    public Geek getById(int id) throws Exception {

        OpUniqueR<Geek> op = new OpUniqueR<Geek>(SELECT_BY_ID_SQL, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, 3);
            }
        };
        Geek g = null;
        g = DataAccessManager.getInstance().queryUnique(op, Geek.class);
        return g;
    }

    public int insert(final Geek g) throws Exception {
        OpUpdate op = new OpUpdate(INSERT_SQL, Keys.GEEK_SINGLE) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setString(1, g.getName());
                ps.setInt(2, g.getStatus());
                ps.setDouble(3, g.getHeight());
                ps.setFloat(4, g.getWeight());
                ps.setDate(5, g.getBirth());
                ps.setTimestamp(6, g.getLastLogin());
                ps.setLong(7, g.getScore());

            }
        };

        int id = DataAccessManager.getInstance().insertAndReturnId(op);
        return id;

    }

}
