/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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

import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.*;
import org.teamapps.universaldb.index.numeric.NumericFilter;
import org.teamapps.universaldb.index.text.TextFilter;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ViewTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Test
	public void testView() {
//		FieldTest test1 = FieldTest.create()
//				.setIntField(1773323)
//				.setTextField("abc11")
//				.save();
//
//		FieldTestView view = FieldTestView.getById(test1.getId());
//		assertEquals(1773323, view.getIntField());
//		assertEquals("abc11", view.getTextField());
//
//		assertTrue(FieldTestView.getCount() > 0);
	}

	@Test
	public void testView2() {
//		Person person = Person.create().setLastName("last144").setFirstName("Hans").save();
//		String companyName = "X114455";
//		Company company = Company.create().setName(companyName).addEmployees(person).save();
//
//		CompanyView companyView = CompanyView.filter().name(TextFilter.textEqualsFilter(companyName)).executeExpectSingleton();
//		assertEquals(company.getId(), companyView.getId());
//		assertEquals(company.getEmployeesCount(), companyView.getEmployeesCount());
//		assertEquals(company.getEmployees().get(0).getLastName(),companyView.getEmployees().get(0).getLastName());
//
//		List<PersonView2> persons = PersonView2.filter().company(NumericFilter.equalsFilter(companyView.getId())).execute();
//		assertEquals(1, persons.size());
//		assertEquals(person.getLastName(), persons.get(0).getLastName());
	}
}
