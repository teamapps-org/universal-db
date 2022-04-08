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

import org.junit.Test;
import org.teamapps.universaldb.schema.Database;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.schema.Table;
import org.teamapps.universaldb.schema.TableOption;

import java.io.IOException;

import static org.junit.Assert.*;

public class SchemaTest {

	@Test
	public void testSchema() throws IOException {
		Schema schema = Schema.create();
		Database database = schema.addDatabase("testDb1");
		Table table = database.addTable("fieldTest", TableOption.CHECKPOINTS, TableOption.HIERARCHY, TableOption.TRACK_CREATION, TableOption.TRACK_MODIFICATION, TableOption.KEEP_DELETED);
		table
				.addBoolean("booleanField")
				.addShort("shortField")
				.addInteger("intField")
				.addLong("longField")
				.addFloat("floatField")
				.addDouble("doubleField")
				.addText("textField")
				.addTimestamp("timestampField")
				.addTime("timeField")
				.addDate("dateField")
				.addDateTime("dateTimeField")
				.addLocalDate("localDateField")
				.addEnum("enumField", "firstValue", "secondValue", "thirdValue", "forthValue", "fifthValue")
				.addFile("fileField")
				.addBinary("binaryField")
				.addReference("singleReferenceField", table, false, "backRefSingleReferenceField")
				.addReference("backRefSingleReferenceField", table, false, "singleReferenceField")
				.addReference("singleReferenceNoBackRefField", table, false)
				.addReference("multiReferenceField", table, true, "backRefMultiReferenceField")
				.addReference("backRefMultiReferenceField", table, true, "multiReferenceField")
				.addReference("parent", table, false, "children")
				.addReference("children", table, true, "parent");


		String schemaDef = schema.getSchemaDefinition();
		Schema schema2 = Schema.parse(schemaDef);
		String schemaDef2 = schema2.getSchemaDefinition();
		assertEquals(schemaDef, schemaDef2);
		assertTrue(schema.isSameSchema(schema));
		assertTrue(schema.isCompatibleWith(schema2));
	}

	@Test
	public void testSchemaMerging() {
		Schema schema = Schema.create();
		Database database = schema.addDatabase("test");
		Table table = database.addTable("test", TableOption.CHECKPOINTS, TableOption.HIERARCHY, TableOption.TRACK_CREATION, TableOption.TRACK_MODIFICATION, TableOption.KEEP_DELETED);
		table
				.addBoolean("booleanField")
				.addShort("shortField")
				.addInteger("intField")
				.addLong("longField")
				.addFloat("floatField")
				.addDouble("doubleField")
				.addText("textField")
				.addTimestamp("timestampField")
				.addTime("timeField")
				.addDate("dateField")
				.addDateTime("dateTimeField")
				.addLocalDate("localDateField")
				.addEnum("enumField", "firstValue", "secondValue", "thirdValue", "forthValue", "fifthValue")
				.addFile("fileField")
				.addBinary("binaryField")
				.addReference("singleReferenceField", table, false, "backRefSingleReferenceField")
				.addReference("backRefSingleReferenceField", table, false, "singleReferenceField")
				.addReference("singleReferenceNoBackRefField", table, false)
				.addReference("multiReferenceField", table, true, "backRefMultiReferenceField")
				.addReference("backRefMultiReferenceField", table, true, "multiReferenceField")
				.addReference("parent", table, false, "children")
				.addReference("children", table, true, "parent");

		Schema schema1 = copySchema(schema);

		assertTrue(isSameSchema(schema, schema1));
		assertTrue(schema.isCompatibleWith(schema1));

		schema1.mapSchema();
		assertTrue(schema1.checkModel());
		assertFalse(isSameSchema(schema, schema1));

		Schema schema2 = Schema.create().addDatabase("test").addTable("test").addBoolean("booleanField").addInteger("intField").getDatabase().getSchema();
		assertFalse(isSameSchema(schema1, schema2));
		assertTrue(schema1.isCompatibleWith(schema2));

		Schema schema1b = copySchema(schema1);
		schema1b.merge(schema2);
		assertTrue(isSameSchema(schema1, schema1b));

		Schema schema3 = Schema.create().addDatabase("test").addTable("test").addBoolean("booleanField").addLong("intField").getDatabase().getSchema();
		assertFalse(isSameSchema(schema1, schema3));
		assertFalse(schema1.isCompatibleWith(schema3));

		Schema schema4 = Schema.create().addDatabase("test").addTable("test").addBoolean("booleanField").addLong("longField2").getDatabase().getSchema();
		assertFalse(isSameSchema(schema1, schema4));
		assertTrue(schema1.isCompatibleWith(schema4));

		Schema schema1c = copySchema(schema1);
		schema1c.merge(schema4);
		assertFalse(isSameSchema(schema1, schema1c));
		schema1c.mapSchema();
		assertTrue(schema1c.checkModel());
	}

	@Test
	public void testTableOptions() {
		Schema schema = Schema.create();
		schema.addDatabase("test").addTable("test", TableOption.CHECKPOINTS, TableOption.HIERARCHY, TableOption.TRACK_CREATION, TableOption.KEEP_DELETED);

		Schema schema2 = Schema.create();
		schema2.addDatabase("test").addTable("test", TableOption.CHECKPOINTS, TableOption.HIERARCHY, TableOption.TRACK_CREATION, TableOption.TRACK_MODIFICATION, TableOption.KEEP_DELETED);

		assertNull(schema.getDatabase("test").getTable("test").getColumn(Table.FIELD_MODIFIED_BY));
		assertNull(schema.getDatabase("test").getTable("test").getColumn(Table.FIELD_MODIFICATION_DATE));

		assertNotNull(schema2.getDatabase("test").getTable("test").getColumn(Table.FIELD_MODIFIED_BY));
		assertNotNull(schema2.getDatabase("test").getTable("test").getColumn(Table.FIELD_MODIFICATION_DATE));

		assertFalse(isSameSchema(schema, schema2));
		assertTrue(schema.isCompatibleWith(schema2));

		schema.merge(schema2);
		assertNotNull(schema.getDatabase("test").getTable("test").getColumn(Table.FIELD_MODIFIED_BY));
		assertNotNull(schema.getDatabase("test").getTable("test").getColumn(Table.FIELD_MODIFICATION_DATE));
	}

	private Schema copySchema(Schema schema) {
		return Schema.parse(schema.getSchemaDefinition());
	}

	private boolean isSameSchema(Schema schema1, Schema schema2) {
		try {
			return schema1.isSameSchema(schema2);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Test
	public void testSchemaNamespace() {
		Schema schema = Schema.create();
		schema.setPojoNamespace("org.test");
		Database db = schema.addDatabase("db");
		Table table = db.addTable("table");
		table.addText("text");
		String schemaDef = schema.getSchemaDefinition();
		Schema parsedSchema = Schema.parse(schemaDef);
		assertEquals("org.test", parsedSchema.getPojoNamespace());

	}
}
