package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

public class TestOpMap extends TestCase {

    @Test
    public void testSelectMapFromDB() {
        OpMap<Integer, Person> opMap = new OpMap<Integer, Person>("SELECT * FROM PERSON", "user") {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {

            }

            @Override
            public Person parse(ResultSet rs) throws SQLException {
                Person person = new Person();
                person.setId(rs.getInt("id"));
                person.setName(rs.getString("name"));
                add(person.getId(), person);
                return person;
            }

        };

        try {
            Map<Integer, Person> value = DataAccessManager.getInstance().queryMap(opMap);
            System.out.println(value);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
