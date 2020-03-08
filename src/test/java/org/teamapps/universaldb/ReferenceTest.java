/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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
import org.teamapps.datamodel.testdb1.Company;
import org.teamapps.datamodel.testdb1.Contract;
import org.teamapps.datamodel.testdb1.FieldTest;
import org.teamapps.datamodel.testdb1.Person;

import java.util.List;

public class ReferenceTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Before
	public void cleanTable() {
		Company.getAll().forEach(e -> e.delete());
		Person.getAll().forEach(e -> e.delete());
		Contract.getAll().forEach(e -> e.delete());
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

		Assert.assertEquals(1, Company.getCount());
		Assert.assertEquals(1, Person.getCount());

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

}
