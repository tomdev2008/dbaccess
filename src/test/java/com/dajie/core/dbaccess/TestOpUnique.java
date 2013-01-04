package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import junit.framework.TestCase;

import org.junit.Test;

public class TestOpUnique extends TestCase {

	private static Random rand = new Random(System.currentTimeMillis());

	@Test
	public void testSelectOneFromDB() {
		final int userId = rand.nextInt(100) % 8;

		OpUnique<Person> opUnique = new OpUnique<Person>(
				"SELECT * FROM PERSON WHERE id = ?", "PERSON") {

			@Override
			public void setParam(PreparedStatement ps) throws SQLException {
				ps.setInt(1, userId);
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
