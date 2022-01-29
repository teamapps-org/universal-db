/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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
import org.teamapps.datamodel.testdb1.CascadeTest1;
import org.teamapps.datamodel.testdb1.CascadeTest2;

import static org.junit.Assert.*;

public class CascadeDeleteTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Test
	public void testCascadeDelete() {
		CascadeTest1.getAll().forEach(t -> t.delete());
		CascadeTest2.getAll().forEach(t -> t.delete());

		assertEquals(0, CascadeTest1.getCount());
		assertEquals(0, CascadeTest2.getCount());

		CascadeTest1 test1 = CascadeTest1.create().setName("t1").addRef2s(
				CascadeTest2.create().setName("t2a"),
				CascadeTest2.create().setName("t2b"),
				CascadeTest2.create().setName("t2c")
		).save();

		assertEquals(3, test1.getRef2sCount());
		assertEquals(1, CascadeTest1.getCount());
		assertEquals(3, CascadeTest2.getCount());

		assertTrue(test1.isStored());
		test1.delete();
		assertFalse(test1.isStored());

		assertEquals(0, CascadeTest1.getCount());
		assertEquals(0, CascadeTest2.getCount());


	}


}
