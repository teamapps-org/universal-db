/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.*;
import org.teamapps.universaldb.pojo.Entity;

import java.util.*;
import java.util.stream.Collectors;

public class ReferenceTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Before
	public void cleanTable() {
//		Company.getAll().forEach(Entity::delete);
//		Person.getAll().forEach(Entity::delete);
//		Contract.getAll().forEach(Entity::delete);
	}

	@Test
	public void testUncommittedMultiReferences() {
		Person p0 = Person.create().setLastName("p1");
		p0.save();

		Company c1 = Company.create().setName("c1");
		c1.addEmployees(p0);
		c1.save();

		Person p1 = Person.create().setLastName("p1");
		Person p2 = Person.create().setLastName("p2");
		Person p3 = Person.create().setLastName("p3");

		c1.addEmployees(p1);
		c1.addEmployees(p2);
		c1.addEmployees(p3);

//		Assert.assertEquals(1, Company.getCount());
//		Assert.assertEquals(1, Person.getCount());

		Assert.assertTrue("check references I", TestBase.compareEntities(c1.getEmployees(), p0, p1, p2, p3));
	}


	@Test
	public void testUncommittedMultiReferences2() {
		Company c1 = Company.create().setName("c1");
		Company c2 = Company.create().setName("c2");
		Company c3 = Company.create().setName("c3");

		Person p1 = Person.create().setLastName("p1");
		Person p2 = Person.create().setLastName("p2");
		Person p3 = Person.create().setLastName("p3");

		c1.addEmployees(p1);
		c1.addEmployees(p2);
		c1.addEmployees(p3);

		Assert.assertEquals(0, Company.getCount());
		Assert.assertEquals(0, Person.getCount());
		Assert.assertTrue("check references II", TestBase.compareEntities(c1.getEmployees(), p1, p2, p3));

		c1.save();
		Assert.assertTrue(TestBase.compareEntities(c1.getEmployees(), p1, p2, p3));
		Assert.assertEquals(c1, p1.getCompany());
		Assert.assertEquals(1, Company.getCount());
		Assert.assertEquals(3, Person.getCount());


	}

	@Test
	public void testUncommittedMultiReferences3() {
		Company c1 = Company.create().setName("c1");
		Company c2 = Company.create().setName("c2");
		Company c3 = Company.create().setName("c3");

		Contract xc1 = Contract.create().setTitle("xc1");
		c1.addCompanyContracts(xc1);
		xc1.addCompanies(c2);

		Person p1 = Person.create().setLastName("p1");
		c2.addEmployees(p1);
		c1.save();
		Company company = Company.getById(c1.getId());

		Assert.assertEquals(company, c1);
		Assert.assertEquals(xc1, company.getCompanyContracts().get(0));
		Assert.assertEquals(p1, c2.getEmployees().get(0));
		Assert.assertEquals(1, c2.getEmployeesCount());

	}

	//@Test
	public void testCommittedMultiReferences() {
		Company company = Company.create().setName("company");
		List<Person> persons = new ArrayList<>();
		for (int i = 1; i <= 10_000; i++) {
			persons.add(Person.create().setLastName("" + i).save());
		}
		company.addEmployees(persons.stream().skip(0).limit(5_000).collect(Collectors.toList()));
		company.save();
		Assert.assertEquals(5_000, company.getEmployeesCount());
		company.addEmployees(persons.stream().skip(5_000).limit(5_000).collect(Collectors.toList()));
		company.save();
		Assert.assertEquals(10_000, company.getEmployeesCount());
		int nextId = 1;
		for (Person employee : company.getEmployees()) {
			int id = Integer.parseInt(employee.getLastName());
			Assert.assertEquals("Missing record", nextId, id);
			nextId = id + 1;
		}
		Assert.assertEquals(10_000, company.getEmployeesCount());
		Assert.assertEquals(10_000, company.getEmployees().size());
	}

	//@Test
	public void testCommittedMultiReferences2() {
		Company company = Company.create().setName("company");

		int size = 10_000;
		for (int i = 0; i < size; i++) {
			company.addEmployees(Person.create().setLastName("" + i)).save();
		}
		Assert.assertEquals(10_000, company.getEmployeesCount());
		Assert.assertEquals(10_000, company.getEmployees().size());
	}

	//@Test
	public void testCommittedMultiReferences3() {
		Company company = Company.create().setName("company").save();
		List<Person> persons = new ArrayList<>();
		for (int i = 1; i <= 5_000; i++) {
			persons.add(Person.create().setLastName("" + i).save());
		}
		Set<Person> expected = new HashSet<>();
		for (Person person : persons) {
			expected.add(person);
			company.addEmployees(person).save();
			checkReferenceCount(company, expected);
		}
		System.out.println("next");
		for (Person person : persons) {
			expected.remove(person);
			company.removeEmployees(person).save();
			checkReferenceCount(company, expected);
		}
		System.out.println("other");
		Iterator<Person> personIterator = persons.iterator();
		List<Person> addedPersons = new ArrayList<>();
		while (personIterator.hasNext()) {
			Person person = personIterator.next();
			company.addEmployees(person).save();
			addedPersons.add(person);
			checkReferenceCount(company, expected);
			if (personIterator.hasNext()) {
				person = personIterator.next();
				company.addEmployees(person).save();
				addedPersons.add(person);
				checkReferenceCount(company, expected);
				Person remove = addedPersons.remove(0);
				expected.remove(remove);
				company.removeEmployees(remove).save();
				checkReferenceCount(company, expected);
			}
		}
	}

	//@Test
	public void testCommittedMultiReferences4() {
		Company company = Company.create().setName("company").save();
		List<Person> persons = new ArrayList<>();
		for (int i = 1; i <= 10_000; i++) {
			persons.add(Person.create().setLastName("" + i).save());
		}
		Set<Person> expected = new HashSet<>();
		List<Person> addedPersons = new ArrayList<>();
		for (Person person : persons) {
			expected.add(person);
			long time = System.currentTimeMillis();
			company.addEmployees(person).save();
			addedPersons.add(person);
			checkReferenceCount(company, expected);
		}
		System.out.println("next");
		for (Person person : persons) {
			Person remove = addedPersons.remove(addedPersons.size() - 1);
			expected.remove(remove);
			company.removeEmployees(remove).save();
			checkReferenceCount(company, expected);
		}

	}

	public void checkReferenceCount(Company company, Set<Person> expected) {
		Assert.assertEquals("Wrong references", expected, new HashSet<>(company.getEmployees()));
		Assert.assertEquals("Wrong count of references", expected.size(), company.getEmployeesCount());
		Assert.assertEquals("Wrong number of references", expected.size(), company.getEmployees().size());
	}


	@Test
	public void testSingleReference() {
		EntityA a1 = EntityA.create()
				.setValue("A1")
				.save();

		EntityA a2 = EntityA.create()
				.setValue("A2")
				.save();

		EntityB b1 = EntityB.create()
				.setValue("B1")
				.save();

		EntityB b2 = EntityB.create()
				.setValue("B2")
				.save();

		a1.setEntityB(b1).save();

		Assert.assertEquals("B1", a1.getEntityB().getValue());
		Assert.assertEquals("A1", b1.getEntityA().getValue());

		a1.setEntityB(b2).save();

		Assert.assertEquals("B2", a1.getEntityB().getValue());
		Assert.assertEquals("A1", b2.getEntityA().getValue());
		Assert.assertNull(b1.getEntityA());


	}

}
