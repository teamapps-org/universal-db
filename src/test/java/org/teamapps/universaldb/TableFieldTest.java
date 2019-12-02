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

import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.EnumField;
import org.teamapps.datamodel.testdb1.FieldTest;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.pojo.Entity;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.*;

import static org.junit.Assert.*;

public class TableFieldTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Test
	public void testBooleanField() {
		FieldTest table = FieldTest.create();
		table.setBooleanField(true);
		assertEquals(true, table.getBooleanField());
		table.save();
		assertEquals(true, table.getBooleanField());
	}

	@Test
	public void testShortField() {
		FieldTest table = FieldTest.create();
		table.setShortField((short) 33);
		assertEquals((short) 33, table.getShortField());
		table.save();
		assertEquals((short) 33, table.getShortField());
	}

	@Test
	public void testIntField() {
		FieldTest table = FieldTest.create();
		table.setIntField(2_000_000);
		assertEquals(2_000_000, table.getIntField());
		table.save();
		assertEquals(2_000_000, table.getIntField());
	}

	@Test
	public void testLongField() {
		FieldTest table = FieldTest.create();
		table.setLongField(2_000_000_000_000l);
		assertEquals(2_000_000_000_000l, table.getLongField());
		table.save();
		assertEquals(2_000_000_000_000l, table.getLongField());
	}

	@Test
	public void testFloatField() {
		FieldTest table = FieldTest.create();
		table.setFloatField(123.55f);
		assertEquals(123.55f, table.getFloatField(), 0);
		table.save();
		assertEquals(123.55f, table.getFloatField(), 0);
	}

	@Test
	public void testDoubleField() {
		FieldTest table = FieldTest.create();
		table.setDoubleField(1000_000_000_000.55d);
		assertEquals(1000_000_000_000.55d, table.getDoubleField(), 0);
		table.save();
		assertEquals(1000_000_000_000.55d, table.getDoubleField(), 0);
	}

	@Test
	public void testTextField() {
		FieldTest table = FieldTest.create();
		table.setTextField("abc");
		assertEquals("abc", table.getTextField());
		table.save();
		assertEquals("abc", table.getTextField());
	}

	@Test
	public void testTranslatableTextField() {
		FieldTest table = FieldTest.create();
		table.setTranslatableText(TranslatableText.create("original", "en").setTranslation("translated-de", "de").setTranslation("translated-fr", "fr"));
		assertEquals("original", table.getTranslatableText().getText());
		assertEquals("translated-de", table.getTranslatableText().getText("de"));
		assertEquals("translated-fr", table.getTranslatableText().getText("fr"));
		assertEquals("original", table.getTranslatableText().getText("es"));
		table.save();
		assertEquals("original", table.getTranslatableText().getText());
		assertEquals("translated-de", table.getTranslatableText().getText("de"));
		assertEquals("translated-fr", table.getTranslatableText().getText("fr"));
		assertEquals("original", table.getTranslatableText().getText("es"));
	}

	@Test
	public void testTimeStamp() {
		int epochSeconds = (int) (System.currentTimeMillis() / 1000);
		long epochMillis = epochSeconds * 1000L;
		Instant now = Instant.ofEpochSecond(epochSeconds);

		FieldTest table = FieldTest.create();
		table.setTimestampField(now);
		assertEquals(now, table.getTimestampField());
		table.save();
		assertEquals(now, table.getTimestampField());
		assertEquals(epochSeconds, table.getTimestampFieldAsEpochSecond());
		table.setTimestampField(null);
		table.setTimestampFieldAsEpochSecond(epochSeconds);
		assertEquals(epochSeconds, table.getTimestampFieldAsEpochSecond());
		table.setTimestampField(null);
		table.setTimestampFieldAsEpochMilli(epochMillis);
		assertEquals(epochMillis, table.getTimestampFieldAsEpochMilli());
	}

	@Test
	public void testTimeField() {
		int epochSeconds = (int) (System.currentTimeMillis() / 1000);
		Instant now = Instant.ofEpochSecond(epochSeconds);

		FieldTest table = FieldTest.create();
		table.setTimeField(now);
		assertEquals(now, table.getTimeField());
		table.save();
		assertEquals(now, table.getTimeField());
		assertEquals(epochSeconds, table.getTimeFieldAsSeconds());
		table.setTimeField(null);
		table.setTimeFieldAsSeconds(epochSeconds);
		assertEquals(epochSeconds, table.getTimeFieldAsSeconds());
	}

	@Test
	public void testDateField() {
		int epochSeconds = (int) (System.currentTimeMillis() / 1000);
		long epochMillis = epochSeconds * 1000L;
		Instant now = Instant.ofEpochSecond(epochSeconds);

		FieldTest table = FieldTest.create();
		table.setDateField(now);
		assertEquals(now, table.getDateField());
		table.save();
		assertEquals(now, table.getDateField());
		assertEquals(epochMillis, table.getDateFieldAsEpochMilli());
		table.setDateField(null);
		table.setDateFieldAsEpochMilli(epochMillis);
		assertEquals(epochMillis, table.getDateFieldAsEpochMilli());
	}

	@Test
	public void testDateTimeField() {
		int epochSeconds = (int) (System.currentTimeMillis() / 1000);
		long epochMillis = epochSeconds * 1000L;
		Instant now = Instant.ofEpochSecond(epochSeconds);

		FieldTest table = FieldTest.create();
		table.setDateTimeField(now);
		assertEquals(now, table.getDateTimeField());
		table.save();
		assertEquals(now, table.getDateTimeField());
		assertEquals(epochMillis, table.getDateTimeFieldAsEpochMilli());
		table.setDateTimeField(null);
		table.setDateTimeFieldAsEpochMilli(epochMillis);
		assertEquals(epochMillis, table.getDateTimeFieldAsEpochMilli());
	}

	@Test
	public void testLocalDateField() {
		Instant now = Instant.ofEpochMilli(System.currentTimeMillis());
		LocalDate localDate = LocalDate.now();
		int epochSeconds = (int) localDate.toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC);
		long epochMillis = epochSeconds * 1000L;

		FieldTest table = FieldTest.create();
		table.setLocalDateField(localDate);
		assertEquals(localDate, table.getLocalDateField());
		table.save();
		assertEquals(localDate, table.getLocalDateField());
		assertEquals(epochMillis, table.getLocalDateFieldAsEpochMilli());
		table.setLocalDateField(null);
		table.setLocalDateFieldAsEpochMilli(epochMillis);
		assertEquals(epochMillis, table.getLocalDateFieldAsEpochMilli());
	}

	@Test
	public void testEnumField() {
		FieldTest table = FieldTest.create();
		table.setEnumField(EnumField.FORTH_VALUE);
		assertEquals(EnumField.FORTH_VALUE, table.getEnumField());
		table.save();
		assertEquals(EnumField.FORTH_VALUE, table.getEnumField());
		table.setEnumField(null);
		assertEquals(null, table.getEnumField());
	}

	@Test
	public void testFileField() throws IOException {
		File tempFile = TestBase.createResourceFile();
		FieldTest table = FieldTest.create();
		table.setFileField(tempFile);
		assertEquals(tempFile.length(), table.getFileField().retrieveFile().length());
		table.save();
		assertEquals(tempFile.length(), table.getFileField().retrieveFile().length());
		assertTrue(table.getFileField().getMetaData() != null);
		assertTrue(table.getFileField().getMetaData().getMetaDataProperty("Author").equals("Matthias Bernstein"));
	}

	@Test
	public void testBinaryField() throws IOException {
		FieldTest record = FieldTest.create();
		byte[] bytes = {123, 1, 101, 64, 44, 33};
		record.setBinaryField(bytes);
		assertEquals(bytes.length, record.getBinaryField().length);
		record.save();
		assertEquals(bytes.length, record.getBinaryField().length);
		assertEquals(bytes[4], record.getBinaryField()[4]);
		assertEquals(6, record.getBinaryFieldLength());
		assertNotNull(record.getBinaryFieldInputStreamSupplier());
	}


		@Test
	public void testSingleReference() throws IOException {
		FieldTest table1 = FieldTest.create().setTextField("Test").save();
		FieldTest table2 = FieldTest.create();
		table2.setSingleReferenceField(table1);
		assertEquals(table1, table2.getSingleReferenceField());
		table2.save();
		assertEquals(table1, table2.getSingleReferenceField());
		assertEquals(table2, table1.getBackRefSingleReferenceField());
		assertEquals("Test", table2.getSingleReferenceField().getTextField());
	}

	@Test
	public void testMultiReference() throws IOException {
		FieldTest table1 = FieldTest.create().setTextField("Test1").save();
		FieldTest table2 = FieldTest.create().setTextField("Test2").save();
		FieldTest table3 = FieldTest.create().setTextField("Test3").save();
		FieldTest table4 = FieldTest.create();
		List<FieldTest> refs = Arrays.asList(table1, table2, table3);
		table4.addMultiReferenceField(table1, table2, table3);
		assertEquals(3, table4.getMultiReferenceField().size());
		table4.save();
		assertEquals(3, table4.getMultiReferenceField().size());
		assertTrue(compareEntityLists(refs, table4.getMultiReferenceField()));
		assertEquals(1, table1.getBackRefMultiReferenceField().size());
		assertEquals(1, table2.getBackRefMultiReferenceField().size());
		assertEquals(1, table3.getBackRefMultiReferenceField().size());
		assertTrue(compareEntityLists(Collections.singletonList(table4), table1.getBackRefMultiReferenceField()));
		assertTrue(compareEntityLists(Collections.singletonList(table4), table2.getBackRefMultiReferenceField()));
		assertTrue(compareEntityLists(Collections.singletonList(table4), table3.getBackRefMultiReferenceField()));

		table3.removeAllBackRefMultiReferenceField().save();
		refs = Arrays.asList(table1, table2);
		assertTrue(compareEntityLists(refs, table4.getMultiReferenceField()));

		table2.setMultiReferenceField(table1, table4).save();
		refs = Arrays.asList(table1, table4);
		assertTrue(compareEntityLists(refs, table2.getMultiReferenceField()));
		refs = Arrays.asList(table1, table2);
		assertTrue(compareEntityLists(refs, table4.getMultiReferenceField()));

	}

	private boolean compareEntityLists(List<? extends Entity> list1, List<? extends Entity> list2) {
		if (list1.size() != list2.size()) {
			return false;
		}
		Set<? extends Entity> set = new HashSet<>(list2);
		for (Entity entity : list1) {
			if (!set.contains(entity)) {
				return false;
			}
		}
		return true;
	}






}
