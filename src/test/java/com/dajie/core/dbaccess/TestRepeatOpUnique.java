package com.dajie.core.dbaccess;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.Test;

public class TestRepeatOpUnique extends TestCase {
	private static Random rand = new Random(System.currentTimeMillis());

	@Test
	public void testSelectOneFromDB() {
		final int repeat = 60;
		for (int i = 0; i < repeat; ++i) {
			final int userId = rand.nextInt(100) % 3;
			String bizName = "user";
			OpUnique<Person> opUnique = new OpUnique<Person>(
					"SELECT * FROM PERSON WHERE id = ?", bizName) {

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
				Person p = DataAccessManager.getInstance()
						.queryUnique(opUnique);
				System.out.println("loop:" + i + "\t" + p);
			} catch (Exception e) {
				e.printStackTrace();
			}
			// sleep
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
