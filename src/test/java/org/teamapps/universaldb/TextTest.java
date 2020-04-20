package org.teamapps.universaldb;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.FieldTest;

import static org.junit.Assert.assertEquals;

public class TextTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Test
	public void testLargeText() {
		StringBuilder sb = new StringBuilder();
		int size = 20_000_000;
		for (int i = 0; i < size / 4; i++) {
			sb.append("abc ");
		}
		FieldTest test = FieldTest.create().setTextField(sb.toString()).save();
		assertEquals(size, test.getTextField().length());
	}
}
