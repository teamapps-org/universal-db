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
package org.teamapps.universaldb.schema;

import org.teamapps.universaldb.*;
import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.index.MappedObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Table implements MappedObject {
	public final static String FIELD_CHECKPOINTS = "metaLastTransactionId";
	public final static String FIELD_VERSIONING = "metaLastTransactionPosition";
	public final static String FIELD_HIERARCHY = "metaHierarchy";
	public final static String FIELD_CREATION_DATE = "metaCreationDate";
	public final static String FIELD_CREATED_BY = "metaCreatedBy";
	public final static String FIELD_MODIFICATION_DATE = "metaModificationDate";
	public final static String FIELD_MODIFIED_BY = "metaModifiedBy";
	public final static String FIELD_DELETION_DATE = "metaDeletionDate";
	public final static String FIELD_DELETED_BY = "metaDeletedBy";

	public static final String[] FORBIDDEN_COLUMN_NAMES = new String[]{FIELD_CHECKPOINTS, FIELD_VERSIONING, FIELD_HIERARCHY, FIELD_CREATION_DATE, FIELD_CREATED_BY, FIELD_MODIFICATION_DATE, FIELD_MODIFIED_BY, FIELD_DELETION_DATE, FIELD_DELETED_BY};

	public static boolean isReservedMetaName(String name) {
		for (String columnName : FORBIDDEN_COLUMN_NAMES) {
			if (name.equals(columnName)) {
				return true;
			}
		}
		return false;
	}

	private final Database database;
	private final String name;
	private final TableConfig tableConfig;
	private final List<Column> columns = new ArrayList<>();
	private int mappingId;


	public Table(Database database, String name, TableConfig tableConfig) {
		this.database = database;
		this.name = name;
		this.tableConfig = tableConfig;

		if (tableConfig.isCheckpoints()) {
			addLong(FIELD_CHECKPOINTS);
		}
		if (tableConfig.isVersioning()) {
			addLong(FIELD_VERSIONING);
		}
		if (tableConfig.isHierarchy()) {
			addInteger(FIELD_HIERARCHY);
		}
		if (tableConfig.trackCreation()) {
			addTimestamp(FIELD_CREATION_DATE);
			addInteger(FIELD_CREATED_BY);
		}
		if (tableConfig.trackModification()) {
			addTimestamp(FIELD_MODIFICATION_DATE);
			addInteger(FIELD_MODIFIED_BY);
		}
		if (tableConfig.keepDeleted()) {
			addTimestamp(FIELD_DELETION_DATE);
			addInteger(FIELD_DELETED_BY);
		}
	}

	public Table addBoolean(String name) {
		addColumn(name, ColumnType.BOOLEAN);
		return this;
	}

	public Table addShort(String name) {
		addColumn(name, ColumnType.SHORT);
		return this;
	}

	public Table addInteger(String name) {
		addColumn(name, ColumnType.INT);
		return this;
	}

	public Table addLong(String name) {
		addColumn(name, ColumnType.LONG);
		return this;
	}

	public Table addFloat(String name) {
		addColumn(name, ColumnType.FLOAT);
		return this;
	}

	public Table addDouble(String name) {
		addColumn(name, ColumnType.DOUBLE);
		return this;
	}

	public Table addText(String name) {
		addColumn(name, ColumnType.TEXT);
		return this;
	}

	public Table addTranslatableText(String name) {
		addColumn(name, ColumnType.TRANSLATABLE_TEXT);
		return this;
	}

	public Table addFile(String name) {
		addColumn(name, ColumnType.FILE);
		return this;
	}

	public Table addBinary(String name) {
		addColumn(name, ColumnType.BINARY);
		return this;
	}

	public Table addTimestamp(String name) {
		addColumn(name, ColumnType.TIMESTAMP);
		return this;
	}

	public Table addDate(String name) {
		addColumn(name, ColumnType.DATE);
		return this;
	}

	public Table addTime(String name) {
		addColumn(name, ColumnType.TIME);
		return this;
	}

	public Table addDateTime(String name) {
		addColumn(name, ColumnType.DATE_TIME);
		return this;
	}

	public Table addLocalDate(String name) {
		addColumn(name, ColumnType.LOCAL_DATE);
		return this;
	}

	public Table addReference(String name, Table referencedTable, boolean multiReference) {
		return addReference(name, referencedTable, multiReference, null);
	}

	public Table addReference(String name, Table referencedTable, boolean multiReference, String backReference) {
		Column column = addColumn(name, multiReference ? ColumnType.MULTI_REFERENCE : ColumnType.SINGLE_REFERENCE);
		column.setReferencedTable(referencedTable);
		column.setBackReference(backReference);
		return this;
	}

	public Table addEnum(String name, String ... values) {
		Column column = addColumn(name, ColumnType.ENUM);
		column.setEnumValues(Arrays.asList(values));
		return this;
	}

	public Column addColumn(String name, ColumnType columnType) {
		Schema.checkName(name);

		Column column = new Column(this, name, columnType);
		columns.add(column);
		return column;
	}

	public Database getDatabase() {
		return database;
	}

	public String getName() {
		return name;
	}

	public TableConfig getTableConfig() {
		return tableConfig;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public Column addColumn(Column column) {
		columns.add(column);
		return column;
	}

	public Column getColumn(String name) {
		return columns.stream()
				.filter(column -> column.getName().equals(name))
				.findAny()
				.orElse(null);
	}

	@Override
	public String getFQN() {
		return getDatabase().getFQN() + "." + name;
	}

	public int getMappingId() {
		return mappingId;
	}

	public void setMappingId(int mappingId) {
		this.mappingId = mappingId;
	}

	public String createDefinition() {
		StringBuilder sb = new StringBuilder();
		sb.append("\t").append(name).append(" as TABLE ").append(tableConfig.writeConfig()).append("\n");
		columns.forEach(column -> sb.append(column.createDefinition()));
		return sb.toString();
	}

	public boolean isCompatibleWith(Table table) {
		if (getMappingId() > 0 && table.getMappingId() > 0 && getMappingId() != table.getMappingId()) {
			return false;
		}
		for (Column column : table.getColumns()) {
			Column localColumn = getColumn(column.getName());
			if (localColumn != null) {
				if (localColumn.getType() != column.getType()) {
					return false;
				}
				if (localColumn.getMappingId() > 0 && column.getMappingId() > 0 && localColumn.getMappingId() != column.getMappingId()) {
					return false;
				}
			} else {
				if (column.getMappingId() != 0 && getDatabase().getSchema().getMappingIds().contains(column.getMappingId())) {
					return false;
				}
			}
		}
		return true;
	}

	public void merge(Table table) {
		for (Column column : table.getColumns()) {
			Column localColumn = getColumn(column.getName());
			if (localColumn == null) {
				addColumn(column);
			} else {
				if (localColumn.getMappingId() == 0) {
					localColumn.setMappingId(column.getMappingId());
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("collection: ").append(name).append(", id:").append(mappingId).append("\n");
		for (Column column : columns) {
			sb.append("\t").append(column.toString()).append("\n");
		}
		return sb.toString();
	}

}
