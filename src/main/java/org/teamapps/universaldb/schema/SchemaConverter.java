/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

		Map<Table, TableModel> tableModelByTable = new HashMap<>();
		Map<TableModel, Table> tableByTableModel = new HashMap<>();
		//Map<FieldModel, Column> columnByFieldModel =new HashMap<>();

		for (Database database : schema.getDatabases()) {
			DatabaseModel model = new DatabaseModel(database.getName(), database.getName(), pojoNamespace, schemaName);
			models.add(model);
			for (Table table : database.getAllTables()) {
				if (table.isView()) {
					String[] parts = table.getReferencedTablePath().split("\\.");
					String remoteDatabaseName = parts[0];
					String remoteTableName = parts[1];
					String remoteView = remoteTableName.endsWith("View") ? remoteTableName :  remoteTableName + "View";
					if (model.getTable(remoteTableName) == null) {
						TableModel remoteTable = model.createRemoteTable(remoteView, remoteView, remoteTableName, remoteDatabaseName, schema.getPojoNamespace());
						tableModelByTable.put(table, remoteTable);
						tableByTableModel.put(remoteTable, table);
					} else {
						TableModel remoteTable = model.createRemoteTable(table.getName(), table.getName(), remoteTableName, remoteDatabaseName, schema.getPojoNamespace());
						tableModelByTable.put(table, remoteTable);
						tableByTableModel.put(remoteTable, table);
					}
				} else {
					TableModel tableModel = model.createTable(table.getName(), table.getName(), table.getTableConfig().trackModification(), table.getTableConfig().keepDeleted(), table.getTableConfig().keepDeleted());
					tableModelByTable.put(table, tableModel);
					tableByTableModel.put(tableModel, table);
				}
			}
			for (Table table : database.getAllTables()) {
				TableModel tbl = tableModelByTable.get(table);
				for (Column column : table.getColumns()) {
					if (!Table.isReservedMetaName(column.getName())) {
						ColumnType columnType = column.getType();
						switch (columnType) {
							case BOOLEAN -> {
								tbl.addBoolean(column.getName());
							}
							case BITSET_BOOLEAN -> {
							}
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
								TableModel referencedTable = tableModelByTable.get(column.getReferencedTable());
								if (referencedTable == null) {
									System.out.println("Missing table:" + column.getReferencedTable().getName());
								}
								tbl.addReference(column.getName(), column.getName(), referencedTable, column.isCascadeDeleteReferences());
							}
							case MULTI_REFERENCE -> {
								TableModel referencedTable = tableModelByTable.get(column.getReferencedTable());
								if (referencedTable == null) {
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
							case CURRENCY -> {
							}
							case DYNAMIC_CURRENCY -> {
							}
						}
					}
				}
			}
			for (Table table : database.getTables()) {
				TableModel tbl = tableModelByTable.get(table);
				for (Column column : table.getColumns()) {
					if (column.getReferencedTable() != null && column.getBackReference() != null) {
						Table referencedTable = column.getReferencedTable();
						Column backReferenceColumn = referencedTable.getColumn(column.getBackReference());
						if (backReferenceColumn.getBackReference() == null) {
							System.out.println("Error: missing backreference old model:" + column.getFQN() + " -> " + backReferenceColumn.getFQN() + ", back-ref:" + backReferenceColumn.getBackReference());
							continue;
						} else if (!backReferenceColumn.getBackReference().equals(column.getName())) {
							System.out.println("Error: wrong back reference name in old model:" + column.getFQN() + " -> " + backReferenceColumn.getFQN() + ", back-ref:" + backReferenceColumn.getBackReference());
							continue;
						} else if (!backReferenceColumn.getReferencedTable().getName().equals(table.getName())) {
							System.out.println("Error: wrong back reference table in old model:" + column.getFQN() + " -> " + backReferenceColumn.getFQN() + ", back-ref:" + backReferenceColumn.getBackReference());
							continue;
						}
						TableModel refTable = model.getTable(referencedTable.getName());

						model.addReverseReferenceField(tbl, column.getName(), refTable, column.getBackReference());
					}
				}
			}
			model.initialize(tableModel -> {
				Table table = tableByTableModel.get(tableModel);
				return table.getMappingId();
			}, (tableModel, fieldModel) -> {
				Table table = tableByTableModel.get(tableModel);
				Column column = table.getColumns().stream().filter(c -> c.getName().equalsIgnoreCase(fieldModel.getName())).findFirst().orElse(null);
				return column.getMappingId();
			});
		}
		return models;
	}

}
