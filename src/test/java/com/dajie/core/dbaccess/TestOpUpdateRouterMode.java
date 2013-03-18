package com.dajie.core.dbaccess;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(OrderedRunner.class)
public class TestOpUpdateRouterMode extends TestCase {

    private static int INSERT_ID = -1;

    private static String bizName = "user_split";

    private static final int MODE = 100;

    @Test
    @Order(order = 1)
    public void testInsertInto() {
        int userId = 286162347;
        int suffix = userId % MODE;
        String tableName = "PERSON_" + suffix;
        String sql = "INSERT INTO " + tableName + "(name, age, birth) values(?, ?, ?);";
        final String userName = UUID.randomUUID().toString();
        OpUpdate op = new OpUpdate(sql, bizName, suffix) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setString(1, userName);
                ps.setInt(2, 18);
                ps.setDate(3, new Date(System.currentTimeMillis()));
            }

            @Override
            public Integer parse(ResultSet rs) throws SQLException {
                return rs.getInt(1);
            }
        };
        try {
            INSERT_ID = DataAccessManager.getInstance().insertAndReturnId(op);
            System.out.println("INSERT_ID:" + INSERT_ID);
            Assert.assertNotEquals(INSERT_ID, -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(order = 2)
    public void testGetLastInsert() {
        Assert.assertNotEquals(INSERT_ID, -1);

        int userId = 286162347;
        int suffix = userId % MODE;
        String tableName = "PERSON_" + suffix;

        OpUnique<Person> opUnique = new OpUnique<Person>("SELECT * FROM " + tableName
                + " WHERE id = ?", bizName, suffix) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, INSERT_ID);
            }

            @Override
            public Person parse(ResultSet rs) throws SQLException {
                Person person = new Person();
                person.setId(rs.getInt("id"));
                person.setName(rs.getString("name"));
                return person;
            }

        };

        try {
            Person p = DataAccessManager.getInstance().queryUnique(opUnique);
            System.out.println(p);
            Assert.assertNotNull(p);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(order = 3)
    public void testDeleteLastInsert() {
        Assert.assertNotEquals(INSERT_ID, -1);
        int userId = 286162347;
        int suffix = userId % MODE;
        String tableName = "PERSON_" + suffix;
        String sql = "DELETE FROM " + tableName + " WHERE id = ?";
        OpUpdate op = new OpUpdate(sql, bizName, suffix) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, INSERT_ID);
            }

            @Override
            public Integer parse(ResultSet rs) throws SQLException {
                return rs.getInt(1);
            }
        };
        try {
            boolean result = DataAccessManager.getInstance().update(op);
            Assert.assertTrue(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(order = 4)
    public void testGetAfterDelete() {
        Assert.assertNotEquals(INSERT_ID, -1);
        OpUnique<Person> opUnique = new OpUnique<Person>("SELECT * FROM PERSON WHERE id = ?",
                bizName) {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {
                ps.setInt(1, INSERT_ID);
            }

            @Override
            public Person parse(ResultSet rs) throws SQLException {
                Person person = new Person();
                person.setId(rs.getInt("id"));
                person.setName(rs.getString("name"));
                return person;
            }

        };

        try {
            Person p = DataAccessManager.getInstance().queryUnique(opUnique);
            Assert.assertNull(p);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
