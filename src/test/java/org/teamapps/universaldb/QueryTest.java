/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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
import org.teamapps.datamodel.testdb1.EnumField;
import org.teamapps.datamodel.testdb1.FieldTest;
import org.teamapps.universaldb.index.bool.BooleanFilter;
import org.teamapps.universaldb.index.enumeration.EnumFilter;
import org.teamapps.universaldb.index.enumeration.EnumFilterType;
import org.teamapps.universaldb.index.file.FileDataField;
import org.teamapps.universaldb.index.file.FileFilter;
import org.teamapps.universaldb.index.numeric.NumericFilter;
import org.teamapps.universaldb.index.text.TextFilter;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.index.translation.TranslatableTextFilter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.teamapps.universaldb.TestBase.check;

public class QueryTest {

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
	public void testNumericQuery() {

		int size = 1000;
		long longOffset = 1000_000_000_000L;

		for (int i = 1; i <= size; i++) {
			FieldTest table = FieldTest.create();
			table.setShortField((short) i);
			table.setIntField(i);
			table.setLongField(longOffset + i);
			table.setTextField("Test" + i + " abc");
			table.save();
		}
		List<FieldTest> values;

		values = FieldTest.filter().intField(NumericFilter.containsFilter(1, 3, 7)).execute();
		assertTrue(check(values, 1, 3, 7));

		values = FieldTest.filter().intField(NumericFilter.equalsFilter(999)).execute();
		assertTrue(check(values, 999));

		values = FieldTest.filter().intField(NumericFilter.notEqualsFilter(999)).execute();
		assertEquals(999, values.size());

		values = FieldTest.filter().intField(NumericFilter.betweenFilter(100, 104)).execute();
		assertTrue(check(values, 100, 101, 102, 103, 104));

		values = FieldTest.filter().intField(NumericFilter.betweenExclusiveFilter(100, 104)).execute();
		assertTrue(check(values, 101, 102, 103));

		values = FieldTest.filter().intField(NumericFilter.greaterFilter(998)).execute();
		assertTrue(check(values, 999, 1000));

		values = FieldTest.filter().intField(NumericFilter.greaterEqualsFilter(998)).execute();
		assertTrue(check(values, 998, 999, 1000));

		values = FieldTest.filter().intField(NumericFilter.smallerFilter(3)).execute();
		assertTrue(check(values, 1, 2));

		values = FieldTest.filter().intField(NumericFilter.smallerEqualsFilter(3)).execute();
		assertTrue(check(values, 1, 2, 3));


		values = FieldTest.filter().shortField(NumericFilter.containsFilter(1, 3, 7)).execute();
		assertTrue(check(values, 1, 3, 7));

		values = FieldTest.filter().shortField(NumericFilter.equalsFilter(999)).execute();
		assertTrue(check(values, 999));

		values = FieldTest.filter().shortField(NumericFilter.notEqualsFilter(999)).execute();
		assertEquals(999, values.size());

		values = FieldTest.filter().shortField(NumericFilter.betweenFilter(100, 104)).execute();
		assertTrue(check(values, 100, 101, 102, 103, 104));

		values = FieldTest.filter().shortField(NumericFilter.betweenExclusiveFilter(100, 104)).execute();
		assertTrue(check(values, 101, 102, 103));

		values = FieldTest.filter().shortField(NumericFilter.greaterFilter(998)).execute();
		assertTrue(check(values, 999, 1000));

		values = FieldTest.filter().shortField(NumericFilter.greaterEqualsFilter(998)).execute();
		assertTrue(check(values, 998, 999, 1000));

		values = FieldTest.filter().shortField(NumericFilter.smallerFilter(3)).execute();
		assertTrue(check(values, 1, 2));

		values = FieldTest.filter().shortField(NumericFilter.smallerEqualsFilter(3)).execute();
		assertTrue(check(values, 1, 2, 3));


		values = FieldTest.filter().longField(NumericFilter.containsFilter(longOffset + 1, longOffset + 3, longOffset + 7)).execute();
		assertTrue(check(values, 1, 3, 7));

		values = FieldTest.filter().longField(NumericFilter.equalsFilter(longOffset + 999)).execute();
		assertTrue(check(values, 999));

		values = FieldTest.filter().longField(NumericFilter.notEqualsFilter(longOffset + 999)).execute();
		assertEquals(999, values.size());

		values = FieldTest.filter().longField(NumericFilter.betweenFilter(longOffset + 100, longOffset + 104)).execute();
		assertTrue(check(values, 100, 101, 102, 103, 104));

		values = FieldTest.filter().longField(NumericFilter.betweenExclusiveFilter(longOffset + 100, longOffset + 104)).execute();
		assertTrue(check(values, 101, 102, 103));

		values = FieldTest.filter().longField(NumericFilter.greaterFilter(longOffset + 998)).execute();
		assertTrue(check(values, 999, 1000));

		values = FieldTest.filter().longField(NumericFilter.greaterEqualsFilter(longOffset + 998)).execute();
		assertTrue(check(values, 998, 999, 1000));

		values = FieldTest.filter().longField(NumericFilter.smallerFilter(longOffset + 3)).execute();
		assertTrue(check(values, 1, 2));

		values = FieldTest.filter().longField(NumericFilter.smallerEqualsFilter(longOffset + 3)).execute();
		assertTrue(check(values, 1, 2, 3));

		values = FieldTest.filter().longField(NumericFilter.smallerEqualsFilter(longOffset + 3)).intField(NumericFilter.containsFilter(2, 3, 4)).execute();
		assertTrue(check(values, 2, 3));

		values = FieldTest.filter().andOr(FieldTest.filter().longField(NumericFilter.smallerEqualsFilter(longOffset + 3)).intField(NumericFilter.containsFilter(2, 3, 4)), FieldTest.filter().shortField(NumericFilter.greaterFilter(999))).execute();
		assertTrue(check(values, 2, 3, 1000));

		values = FieldTest.filter().textField(TextFilter.textEqualsFilter("Test1 abc")).execute();
		assertTrue(check(values, 1));
	}

	@Test
	public void testFullText() {

		int size = 1000;
		long longOffset = 1000_000_000_000L;

		for (int i = 1; i <= size; i++) {
			FieldTest table = FieldTest.create();
			table.setShortField((short) i);
			table.setIntField(i);
			table.setLongField(longOffset + i);
			table.setTextField("Test" + i + " abc");
			table.save();
		}
		List<FieldTest> values;
		values = FieldTest.filter().textField(TextFilter.textEqualsFilter("Test99 abc")).execute();
		assertTrue(check(values, 99));

		values = FieldTest.filter().textField(TextFilter.termEqualsFilter("Test99")).execute();
		assertTrue(check(values, 99));

		values = FieldTest.filter().textField(TextFilter.termEqualsFilter("abc")).execute();
		assertEquals(1000, values.size());

		values = FieldTest.filter().textField(TextFilter.termStartsWithFilter("Test99")).execute();
		assertEquals(11, values.size());

		values = FieldTest.filter().textField(TextFilter.termStartsWithFilter("Test777")).execute();
		assertTrue(check(values, 777));

		values = FieldTest.filter().textField(TextFilter.termEqualsFilter("Test777")).execute();
		assertTrue(check(values, 777));

		values = FieldTest.filter().textField(TextFilter.termSimilarFilter("Grest777")).execute();
		assertTrue(check(values, 777));

		values = FieldTest.filter().fullTextFilter(TextFilter.termContainsFilter("Test123 abc")).execute();
		assertTrue(check(values, 123));

		values = FieldTest.filter().fullTextFilter(TextFilter.textEqualsFilter("Test123 abc")).execute();
		assertTrue(check(values, 123));


		values = FieldTest.filter().fullTextFilter(TextFilter.termContainsFilter("Test123 abc"), FieldTest.FIELD_TEXT_FIELD).execute();
		assertTrue(check(values, 123));

	}

	@Test
	public void testTranslatableText() {

		int size = 1000;
		long longOffset = 1000_000_000_000L;

		for (int i = 1; i <= size; i++) {
			FieldTest table = FieldTest.create();
			table.setShortField((short) i);
			table.setIntField(i);
			table.setLongField(longOffset + i);
			table.setTextField("Test" + i + " abc");
			table.setTranslatableText(new TranslatableText("En" + i + "xval","en").setTranslation("De" + i + "xval", "de").setTranslation("Fr" + i + "xval", "fr"));
			table.save();
		}
		List<FieldTest> values;

		values = FieldTest.filter().translatableText(TranslatableTextFilter.termContainsFilter("xval", "de")).execute();
		assertEquals(1000, values.size());

		values = FieldTest.filter().translatableText(TranslatableTextFilter.termContainsFilter("de", "de")).execute();
		assertEquals(1000, values.size());

		values = FieldTest.filter().translatableText(TranslatableTextFilter.termContainsFilter("fr", "de")).execute();
		assertEquals(0, values.size());

		values = FieldTest.filter().translatableText(TranslatableTextFilter.termContainsFilter("xval", "xy")).execute();
		assertEquals(1000, values.size());


		values = FieldTest.filter().textField(TextFilter.textEqualsFilter("Test99 abc")).execute();
		assertTrue(check(values, 99));

		values = FieldTest.filter().textField(TextFilter.termEqualsFilter("Test99")).execute();
		assertTrue(check(values, 99));

		values = FieldTest.filter().textField(TextFilter.termEqualsFilter("abc")).execute();
		assertEquals(1000, values.size());

		values = FieldTest.filter().textField(TextFilter.termStartsWithFilter("Test99")).execute();
		assertEquals(11, values.size());

		values = FieldTest.filter().textField(TextFilter.termStartsWithFilter("Test777")).execute();
		assertTrue(check(values, 777));

		values = FieldTest.filter().textField(TextFilter.termEqualsFilter("Test777")).execute();
		assertTrue(check(values, 777));

		values = FieldTest.filter().textField(TextFilter.termSimilarFilter("Grest777")).execute();
		assertTrue(check(values, 777));

		values = FieldTest.filter().fullTextFilter(TextFilter.textEqualsFilter("Test123 abc")).execute();
		assertTrue(check(values, 123));

		values = FieldTest.filter().fullTextFilter(TextFilter.termContainsFilter("Test123 abc")).execute();
		assertTrue(check(values, 123));

		values = FieldTest.filter().fullTextFilter(TextFilter.termContainsFilter("Test123 abc"), FieldTest.FIELD_TEXT_FIELD).execute();
		assertTrue(check(values, 123));

	}


	@Test
	public void testFileQuery() throws IOException {
		File tempFile = TestBase.createResourceFile();
		int size = 10;
		long longOffset = 1000_000_000_000L;

		for (int i = 1; i <= size; i++) {
			FieldTest table = FieldTest.create();
			table.setShortField((short) i);
			table.setIntField(i);
			table.setLongField(longOffset + i);
			table.setTextField("Test" + i + " abc");
			if (i % 5 == 0) {
				table.setFileField(tempFile);
			}
			table.save();
		}
		List<FieldTest> values;

		values = FieldTest.filter().fileField(FileFilter.sizeEquals(12805l)).execute();
		assertTrue(check(values, 5, 10));

		values = FieldTest.filter().fileField(FileFilter.sizeGreater(12000l)).execute();
		assertTrue(check(values, 5, 10));

		values = FieldTest.filter().fileField(FileFilter.sizeGreater(25000)).execute();
		assertTrue(check(values));

		values = FieldTest.filter().fileField(FileFilter.notEmpty()).execute();
		assertTrue(check(values, 5, 10));

		values = FieldTest.filter().fileField(FileFilter.empty()).execute();
		assertTrue(check(values, 1, 2, 3, 4, 6, 7, 8, 9));

		values = FieldTest.filter().fileField(FileFilter.termContains("lore", FileDataField.CONTENT)).execute();
		assertTrue(check(values, 5, 10));

		values = FieldTest.filter().fileField(FileFilter.termContains("temp", FileDataField.NAME)).execute();
		assertTrue(check(values, 5, 10));

		values = FieldTest.filter().fileField(FileFilter.termContains("application/vnd.openxmlformats-officedocument.wordprocessingml.document", FileDataField.MIME_TYPE)).execute();
		assertTrue(check(values, 5, 10));

	}

	@Test
	public void testBooleanQuery() {

		int size = 5;
		for (int i = 1; i <= size; i++) {
			FieldTest table = FieldTest.create();
			table.setBooleanField(i % 2 == 0);
			table.setIntField(i);
			table.save();
		}
		List<FieldTest> values;
		values = FieldTest.filter().booleanField(BooleanFilter.trueFilter()).execute();
		assertTrue(check(values, 2, 4));

		values = FieldTest.filter().booleanField(BooleanFilter.falseFilter()).execute();
		assertTrue(check(values, 1, 3, 5));
	}

	@Test
	public void testEnumQuery() {

		FieldTest.create().setEnumField(EnumField.FIRST_VALUE).setIntField(1).save();
		FieldTest.create().setEnumField(EnumField.SECOND_VALUE).setIntField(2).save();
		FieldTest.create().setEnumField(EnumField.THIRD_VALUE).setIntField(3).save();
		FieldTest.create().setEnumField(EnumField.FORTH_VALUE).setIntField(4).save();
		FieldTest.create().setEnumField(EnumField.FIFTH_VALUE).setIntField(5).save();
		FieldTest.create().setEnumField(null).setIntField(6).save();


		List<FieldTest> values;
		values = FieldTest.filter().enumField(EnumFilterType.EQUALS, EnumField.THIRD_VALUE).execute();
		assertTrue(check(values, 3));

		values = FieldTest.filter().enumField(EnumFilterType.CONTAINS, EnumField.FIFTH_VALUE, EnumField.THIRD_VALUE, EnumField.FIRST_VALUE).execute();
		assertTrue(check(values, 1, 3, 5));

		values = FieldTest.filter().enumField(EnumFilterType.CONTAINS_NOT, EnumField.FIFTH_VALUE, EnumField.THIRD_VALUE, EnumField.FIRST_VALUE).execute();
		assertTrue(check(values, 2, 4, 6));

		values = FieldTest.filter().enumField(EnumFilterType.NOT_EQUALS, EnumField.FIFTH_VALUE).execute();
		assertTrue(check(values, 1, 2, 3, 4, 6));

		values = FieldTest.filter().enumField(EnumFilterType.IS_EMPTY, EnumField.FIFTH_VALUE, EnumField.THIRD_VALUE, EnumField.FIRST_VALUE).execute();
		assertTrue(check(values, 6));

	}

	@Test
	public void testReferenceQuery() {
		FieldTest e1 = FieldTest.create().setTextField("e1").setIntField(1).save();
		FieldTest e2 = FieldTest.create().setTextField("e2").setIntField(2).save();
		FieldTest e3 = FieldTest.create().setTextField("e3").setIntField(3).save();
		FieldTest e4 = FieldTest.create().setTextField("e4").setIntField(4).save();
		FieldTest e5 = FieldTest.create().setTextField("e5").setIntField(5).save();
		FieldTest e6 = FieldTest.create().setTextField("e6").setIntField(6).save();
		FieldTest e7 = FieldTest.create().setTextField("e7").setIntField(7).save();
		FieldTest e8 = FieldTest.create().setTextField("e8").setIntField(8).save();
		FieldTest e9 = FieldTest.create().setTextField("e9").setIntField(9).save();
		FieldTest e10 = FieldTest.create().setTextField("e10").setIntField(10).save();
		FieldTest e11 = FieldTest.create().setTextField("e11").setIntField(11).save();
		FieldTest e12 = FieldTest.create().setTextField("e12").setIntField(12).save();

		e2.setParent(e1).save();
		e3.setParent(e1).save();
		e4.setParent(e2).save();
		e5.setParent(e4).save();
		e6.setParent(e5).save();
		e7.setParent(e6).save();
		e8.setParent(e7).save();
		e9.setParent(e8).save();
		e10.setParent(e2).save();
		e11.setParent(e2).save();
		e11.addChildren(e12).save();

		List<FieldTest> values;
		values = FieldTest.filter().filterParent(FieldTest.filter().textField(TextFilter.textEqualsFilter("e1"))).execute();
		assertTrue(check(values, 2, 3));

		values = FieldTest.filter().filterChildren(FieldTest.filter().intField(NumericFilter.containsFilter(11, 8))).execute();
		assertTrue(check(values, 2, 7));

		values = FieldTest.filter().filterParent(FieldTest.filter().filterParent(FieldTest.filter().textField(TextFilter.textEqualsFilter("e1")))).execute();
		assertTrue(check(values, 4, 10, 11));

	}
}
