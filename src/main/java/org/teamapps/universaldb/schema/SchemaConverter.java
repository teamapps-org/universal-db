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
package org.teamapps.universaldb.schema;

import org.teamapps.universaldb.generator.PojoCodeGenerator;
import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.model.DatabaseModel;
import org.teamapps.universaldb.model.EnumModel;
import org.teamapps.universaldb.model.TableModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SchemaConverter {

	public static String convertLegacySchema(String schemaData) {
		try {
			Schema schema = Schema.parse(schemaData);
			List<DatabaseModel> models = convertSchema(schema);
			if (models.size() > 0) {
				DatabaseModel model = models.get(0);
				String classCode = new PojoCodeGenerator().createModelProviderClassCode(model);
				System.out.println(classCode);
				return classCode;
			}
			return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static List<DatabaseModel> convertSchema(Schema schema) {
		List<DatabaseModel> models = new ArrayList<>();
		String pojoNamespace = schema.getPojoNamespace();
		String schemaName = schema.getSchemaName();

		for (Database database : schema.getDatabases()) {
			DatabaseModel model = new DatabaseModel(database.getName(), database.getName(), pojoNamespace, schemaName);
			models.add(model);
			for (Table table : database.getAllTables()) {
				if (table.isView()) {
					model.createRemoteTable(table.getName(), table.getName(), table.getDatabase().getName(), null);
				} else {
					model.createTable(table.getName(), table.getName(), table.getTableConfig().trackModification(), table.getTableConfig().isVersioning(), table.getTableConfig().keepDeleted());
				}
			}
			for (Table table : database.getAllTables()) {
				TableModel tbl = model.getTable(table.getName());
				for (Column column : table.getColumns()) {
					if (!Table.isReservedMetaName(column.getName())) {
						ColumnType columnType = column.getType();
						switch (columnType) {
							case BOOLEAN -> {
								tbl.addBoolean(column.getName());
							}
							case BITSET_BOOLEAN -> { }
							case SHORT -> tbl.addShort(column.getName());
							case INT -> tbl.addInteger(column.getName());
							case LONG -> tbl.addLong(column.getName());
							case FLOAT -> tbl.addFloat(column.getName());
							case DOUBLE -> tbl.addDouble(column.getName());
							case TEXT -> tbl.addText(column.getName());
							case TRANSLATABLE_TEXT -> tbl.addTranslatableText(column.getName());
							case FILE -> {
								tbl.addFile(column.getName(), column.getName(), true, false);
							}
							case SINGLE_REFERENCE -> {
								TableModel referencedTable = model.getTable(column.getReferencedTable().getName());
								if (referencedTable ==null) {
									System.out.println("Missing table:" + column.getReferencedTable().getName());
								}
								tbl.addReference(column.getName(), column.getName(), referencedTable, column.isCascadeDeleteReferences());
							}
							case MULTI_REFERENCE -> {
								TableModel referencedTable = model.getTable(column.getReferencedTable().getName());
								if (referencedTable ==null) {
									System.out.println("Missing table:" + column.getReferencedTable().getName());
								}
								tbl.addMultiReference(column.getName(), column.getName(), referencedTable, column.isCascadeDeleteReferences());
							}
							case TIMESTAMP -> tbl.addTimestamp(column.getName());
							case DATE -> tbl.addDate(column.getName());
							case TIME -> tbl.addTime(column.getName());
							case DATE_TIME -> tbl.addDateTime(column.getName());
							case LOCAL_DATE -> tbl.addLocalDate(column.getName());
							case ENUM -> {
								List<String> enumValues = column.getEnumValues();
								EnumModel enumModel = model.getEnumModel(column.getName());
								if (enumModel == null) {
									enumModel = model.createEnum(column.getName(), enumValues);
								}
								tbl.addEnum(enumModel);
							}
							case BINARY -> tbl.addByteArray(column.getName());
							case CURRENCY -> { }
							case DYNAMIC_CURRENCY -> { }
						}
					}
				}
			}
			for (Table table : database.getTables()) {
				TableModel tbl = model.getTable(table.getName());
				for (Column column : table.getColumns()) {
					if (column.getReferencedTable() != null && column.getBackReference() != null) {
						TableModel refTable = model.getTable(column.getReferencedTable().getName());
						model.addReverseReferenceField(tbl, column.getName(), refTable, column.getBackReference());
					}
				}
			}
		}
		return models;
	}

}
