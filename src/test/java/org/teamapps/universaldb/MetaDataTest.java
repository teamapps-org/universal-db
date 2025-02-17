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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.datamodel.testdb1.*;
import org.teamapps.universaldb.pojo.Entity;

public class MetaDataTest {

	@BeforeClass
	public static void init() throws Exception {
		TestBase.init();
	}

	@Before
	public void cleanTable() {
		FieldTest.getAll().forEach(Entity::delete);
	}

	@Test
	public void testCreationDate() {
		FieldTest record = FieldTest.create().setTextField("value");
		Assert.assertNull("check created", record.getMetaCreationDate());
		record.save();
		Assert.assertNotNull("check created", record.getMetaCreationDate());
	}

	@Test
	public void testModificationDate() {
		FieldTest record = FieldTest.create().setTextField("value");
		Assert.assertNull("check created", record.getMetaCreationDate());
		Assert.assertNull("check modified", record.getMetaModificationDate());
		record.save();
		Assert.assertNotNull("check created", record.getMetaCreationDate());
		Assert.assertNotNull("check modified", record.getMetaModificationDate()); //TODO

		record.setTextField("value2").save();
		Assert.assertNotNull("check created", record.getMetaCreationDate());
		Assert.assertNotNull("check modified", record.getMetaModificationDate()); //todo change

		int id = record.getId();
		record = FieldTest.getById(id);
		record.setIntField(3).setTextField("value3").save();
		Assert.assertNotNull("check created", record.getMetaCreationDate());
		Assert.assertNotNull("check modified", record.getMetaModificationDate());

		record.setIntField(4).setTextField(null).save();
		Assert.assertNotNull("check created", record.getMetaCreationDate());
		Assert.assertNotNull("check modified", record.getMetaModificationDate());

	}



}
