package org.teamapps.universaldb;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.Company;
import org.teamapps.datamodel.testdb1.Person;
import org.teamapps.datamodel.testdb2.Model2Table;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiDbTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init2();
	}

	@Test
	public void testMultiDb() throws Exception {
		Assert.assertEquals(0, Company.getCount());
		Assert.assertEquals(0, org.teamapps.datamodel.testdb2.Company.getCount());

		Company.create().setName("first").save();
		Company.create().setName("second").save();
		Company.create().setName("third").save();

		Assert.assertEquals(3, Company.getCount());
		Assert.assertEquals(3, org.teamapps.datamodel.testdb2.Company.getCount());

		Person p1 = Person.create().setLastName("p1").save();
		Person p2 = Person.create().setLastName("p2").save();
		Person p3 = Person.create().setLastName("p3").save();
		Person p4 = Person.create().setLastName("p4").save();
		Company c1 = Company.create().setName("c1").addEmployees(p1, p2, p3).save();
		Company c2 = Company.create().setName("c2").addEmployees(p4).save();

		assertTrue("employess of c1", TestBase.compareEntities(c1.getEmployees(), p1, p2, p3));
		assertTrue("employess of c2", TestBase.compareEntities(c2.getEmployees(), p4));
		assertEquals(p1.getCompany(), c1);
		assertEquals(p2.getCompany(), c1);
		assertEquals(p3.getCompany(), c1);
		assertEquals(p4.getCompany(), c2);

		List<org.teamapps.datamodel.testdb2.Person> people = org.teamapps.datamodel.testdb2.Person.getAll();
		List<org.teamapps.datamodel.testdb2.Company> companies = org.teamapps.datamodel.testdb2.Company.getAll();

		for (int i = 0; i < 4; i++) {
			Model2Table.create().setName("Test-" + i)
					.setPerson(people.get(i))
					.setCompany(companies.get(i))
					.save();
		}

		assertEquals("p1", Model2Table.getById(1).getPerson().getLastName());
		assertEquals("p2", Model2Table.getById(2).getPerson().getLastName());
		assertEquals("first", Model2Table.getById(1).getCompany().getName());

		Model2Table.getAll().forEach(System.out::println);

	}

}
