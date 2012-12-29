package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;


public class TestOpList extends TestCase {

	@Test
	public void testSelectAllFromDB() {
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
			e.printStackTrace();
		}
	}

}
