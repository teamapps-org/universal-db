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

		assertTrue(test1.exists());
		test1.delete();
		assertFalse(test1.exists());

		assertEquals(0, CascadeTest1.getCount());
		assertEquals(0, CascadeTest2.getCount());


	}


}
