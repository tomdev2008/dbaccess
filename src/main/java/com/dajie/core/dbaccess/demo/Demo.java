package com.dajie.core.dbaccess.demo;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.dajie.core.dbaccess.DataAccessManager;
import com.dajie.core.dbaccess.OpList;

public class Demo {

    public static void main(String[] args) {
        OpList<Person> op = new OpList<Person>("SELECT * FROM PERSON", "PERSON") {

            @Override
            public void setParam(PreparedStatement ps) throws SQLException {

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
            List<Person> value = DataAccessManager.getInstance().querList(op);
            System.out.println(value);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("done...");
        System.exit(0);
    }
}
