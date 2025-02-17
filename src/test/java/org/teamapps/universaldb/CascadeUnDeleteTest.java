/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
import org.teamapps.universaldb.pojo.Entity;

import static org.junit.Assert.*;

public class CascadeUnDeleteTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Test
	public void testCascadeUnDelete() {
		CascadeTest1.getAll().forEach(Entity::delete);
		CascadeTest2.getAll().forEach(Entity::delete);

		assertEquals(0, CascadeTest1.getCount());
		assertEquals(0, CascadeTest2.getCount());

		CascadeTest2 ref1 = CascadeTest2.create().setName("ref1");
		CascadeTest2 ref2 = CascadeTest2.create().setName("ref2");
		CascadeTest2 ref3 = CascadeTest2.create().setName("ref3");
		CascadeTest2 ref4 = CascadeTest2.create().setName("ref4");
		CascadeTest1 entity = CascadeTest1.create().setName("testCascadeDelete").addRef2s(
				ref1,
				ref2,
				ref3,
				ref4
		).save();
		assertFalse(entity.isDeleted());
		assertTrue(entity.isStored());
		assertTrue(ref1.isStored());
		assertTrue(ref2.isStored());
		assertTrue(ref3.isStored());
		assertTrue(ref4.isStored());
		assertEquals(4, entity.getRef2sCount());
		assertEquals(4, entity.getRef2s().size());
		assertEquals(ref1, entity.getRef2s().get(0));
		assertEquals(ref4, entity.getRef2s().get(3));
		assertEquals(1, CascadeTest1.getCount());
		assertEquals(4, CascadeTest2.getCount());


		entity.delete();

		assertFalse(entity.isStored());
		assertTrue(entity.isDeleted());
		assertTrue(ref1.isDeleted());
		assertTrue(ref2.isDeleted());
		assertTrue(ref3.isDeleted());
		assertTrue(ref4.isDeleted());
		assertEquals(4, entity.getRef2sCount());
		assertEquals(4, entity.getRef2s().size());
		assertEquals(ref1, entity.getRef2s().get(0));
		assertEquals(ref4, entity.getRef2s().get(3));
		assertEquals(0, CascadeTest1.getCount());
		assertEquals(0, CascadeTest2.getCount());

		entity.restoreDeleted();

		assertFalse(entity.isDeleted());
		assertTrue(entity.isStored());
		assertTrue(ref1.isStored());
		assertTrue(ref2.isStored());
		assertTrue(ref3.isStored());
		assertTrue(ref4.isStored());
		assertEquals(4, entity.getRef2sCount());
		assertEquals(4, entity.getRef2s().size());
		assertEquals(ref1, entity.getRef2s().get(0));
		assertEquals(ref4, entity.getRef2s().get(3));
		assertEquals(1, CascadeTest1.getCount());
		assertEquals(4, CascadeTest2.getCount());

	}


	@Test
	public void testCascadeUnDelete2() {
		CascadeTest1.getAll().forEach(Entity::delete);
		CascadeTest2.getAll().forEach(Entity::delete);

		assertEquals(0, CascadeTest1.getCount());
		assertEquals(0, CascadeTest2.getCount());


		CascadeTest2 test2A = CascadeTest2.create().setName("test2A");
		CascadeTest2 test2B = CascadeTest2.create().setName("test2B");
		CascadeTest2 test2C = CascadeTest2.create().setName("test2C");
		CascadeTest2 test2D = CascadeTest2.create().setName("test2D");
		CascadeTest3NoKeep test3 = CascadeTest3NoKeep.create().setName("test3");
		CascadeTest4NoKeep test4 = CascadeTest4NoKeep.create().setName("test3");
		CascadeTest5NoKeep test5 = CascadeTest5NoKeep.create().setName("test3");
		CascadeTest1 entity = CascadeTest1.create().setName("testCascadeDelete")
				.addRef2s(
						test2A,
						test2B,
						test2C,
						test2D
				)
				.setOtherRef2s()
				.setRef3NoKeep(test3)
				.setRef4NoKeep(test4)
				.setRef5NoKeep(test5)
				.save();

		assertFalse(entity.isDeleted());
		assertTrue(entity.isStored());
		assertTrue(test2A.isStored());
		assertTrue(test2B.isStored());
		assertTrue(test2C.isStored());
		assertTrue(test2D.isStored());
		assertTrue(test3.isStored());
		assertTrue(test4.isStored());
		assertTrue(test5.isStored());
		assertEquals(4, entity.getRef2sCount());
		assertEquals(4, entity.getRef2s().size());
		assertEquals(test2A, entity.getRef2s().get(0));
		assertEquals(test2D, entity.getRef2s().get(3));
		assertEquals(1, CascadeTest1.getCount());
		assertEquals(4, CascadeTest2.getCount());

		test5.delete();

		assertFalse(entity.isStored());
		assertTrue(entity.isDeleted());
		assertTrue(test2A.isDeleted());
		assertTrue(test2B.isDeleted());
		assertTrue(test2C.isDeleted());
		assertTrue(test2D.isDeleted());
		assertTrue(test3.isDeleted());
		assertTrue(test4.isDeleted());
		assertTrue(test5.isDeleted());
		assertEquals(4, entity.getRef2sCount());
		assertEquals(4, entity.getRef2s().size());
		assertEquals(test2A, entity.getRef2s().get(0));
		assertEquals(test2D, entity.getRef2s().get(3));
		assertEquals(0, CascadeTest1.getCount());
		assertEquals(0, CascadeTest2.getCount());

		entity.restoreDeleted();

		assertFalse(test5.isStored());
		assertFalse(entity.isDeleted());
		assertTrue(entity.isStored());
		assertTrue(test2A.isStored());
		assertTrue(test2B.isStored());
		assertTrue(test2C.isStored());
		assertTrue(test2D.isStored());
		assertEquals(4, entity.getRef2sCount());
		assertEquals(4, entity.getRef2s().size());
		assertEquals(test2A, entity.getRef2s().get(0));
		assertEquals(test2D, entity.getRef2s().get(3));
		assertEquals(1, CascadeTest1.getCount());
		assertEquals(4, CascadeTest2.getCount());

	}


}
