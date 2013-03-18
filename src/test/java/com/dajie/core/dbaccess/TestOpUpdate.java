package com.dajie.core.dbaccess;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(OrderedRunner.class)
public class TestOpUpdate extends TestCase {

    private static int INSERT_ID = -1;

    private static String bizName = "user";

    @Test
    @Order(order = 1)
    public void testInsertInto() {
        String sql = "INSERT INTO PERSON(name, age, birth) values(?, ?, ?);";
        final String userName = UUID.randomUUID().toString();
        OpUpdate op = new OpUpdate(sql, bizName) {

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
            System.out.println(p);
            Assert.assertNotNull(p);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(order = 3)
    public void testDeleteLastInsert() {
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        Assert.assertNotEquals(INSERT_ID, -1);
        String sql = "DELETE FROM PERSON WHERE id = ?";
        OpUpdate op = new OpUpdate(sql, bizName) {

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
