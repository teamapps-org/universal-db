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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.FieldTest;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.teamapps.universaldb.TestBase.check;

public class SortTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Before
	public void cleanTable() {
		for (FieldTest fieldTest : FieldTest.getAll()) {
			fieldTest.delete();
		}
	}

	@Test
	public void sortTest() {

		int size = 1000;
		long longOffset = 1000_000_000_000L;

		for (int i = 1; i <= size; i++) {
			FieldTest table = FieldTest.create();
			table.setShortField((short) (size + 1 - i));
			table.setIntField(i);
			table.setLongField(longOffset + i);
			table.setTextField("Test" + i + " abc");
			table.save();
		}
		List<FieldTest> values;

		values = FieldTest.filter().execute(FieldTest.FIELD_INT_FIELD, false);
		assertEquals(1000, values.get(0).getIntField());

		values = FieldTest.filter().execute(FieldTest.FIELD_INT_FIELD, true);
		assertEquals(1, values.get(0).getIntField());

		values = FieldTest.filter().execute(FieldTest.FIELD_SHORT_FIELD, false);
		assertEquals(1, values.get(0).getIntField());
	}
}
