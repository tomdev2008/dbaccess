package com.dajie.core.dbaccess;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(OrderedRunner.class)
public class TestOpUpdate extends TestCase {

	private static int INSERT_ID = -1;

	@Test
	@Order(order = 1)
	public void testInsertInto() {
		String sql = "INSERT INTO PERSON(name, age, birth) values(?, ?, ?);";
		String tableName = "PERSON";
		OpUpdate op = new OpUpdate(sql, tableName) {

			@Override
			public void setParam(PreparedStatement ps) throws SQLException {
				ps.setString(1, "张三");
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
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	@Order(order = 2)
	public void testGetLastInsert() {
		Assert.assertNotEquals(INSERT_ID, -1);
		OpUnique<Person> opUnique = new OpUnique<Person>(
				"SELECT * FROM PERSON WHERE id = ?", "PERSON") {

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
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Test
	@Order(order = 3)
	public void testDeleteLastInsert() {
		Assert.assertNotEquals(INSERT_ID, -1);
		String sql = "DELETE FROM PERSON WHERE id = ?";
		String tableName = "PERSON";
		OpUpdate op = new OpUpdate(sql, tableName) {

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
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
