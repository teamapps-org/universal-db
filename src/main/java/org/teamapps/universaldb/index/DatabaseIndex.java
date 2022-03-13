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
package org.teamapps.universaldb.index;

import org.teamapps.universaldb.schema.Database;
import org.teamapps.universaldb.schema.Table;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DatabaseIndex implements MappedObject {

	private final SchemaIndex schemaIndex;
	private final String name;
	private final File dataPath;
	private final File fullTextIndexPath;
	private final List<TableIndex> tables;
	private int mappingId;

	public DatabaseIndex(SchemaIndex schema, String name) {
		this.schemaIndex = schema;
		this.name = name;
		this.dataPath = new File(schema.getDataPath(), name);
		this.fullTextIndexPath = new File(schema.getFullTextIndexPath(), name);
		dataPath.mkdir();
		fullTextIndexPath.mkdir();
		this.tables = new ArrayList<>();
	}

	public File getDataPath() {
		return dataPath;
	}

	public File getFullTextIndexPath() {
		return fullTextIndexPath;
	}

	public SchemaIndex getSchemaIndex() {
		return schemaIndex;
	}

	public void merge(Database database, boolean checkFullTextIndex) {
		Map<String, TableIndex> tableMap = tables.stream().collect(Collectors.toMap(TableIndex::getName, table -> table));
		for (Table table : database.getTables()) {
			TableIndex localTable = tableMap.get(table.getName());
			if (localTable == null) {
				localTable = new TableIndex(this, table, table.getTableConfig());
				addTable(localTable);
			}
			if (localTable.getMappingId() == 0) {
				localTable.setMappingId(table.getMappingId());
			}
			localTable.merge(table);
		}
		if (checkFullTextIndex) {
			for (TableIndex table : tables) {
				table.getRecordVersioningIndex().checkVersionIndex();
				table.checkFullTextIndex();
			}
		}

	}

	public String getName() {
		return name;
	}

	@Override
	public String getFQN() {
		return name;
	}

	public int getMappingId() {
		return mappingId;
	}

	public void setMappingId(int mappingId) {
		this.mappingId = mappingId;
	}

	public TableIndex addTable(TableIndex table) {
		tables.add(table);
		return table;
	}

	public List<TableIndex> getTables() {
		return tables;
	}

	public TableIndex getTable(String name) {
		return tables.stream().filter(table -> table.getName().equals(name)).findAny().orElse(null);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Database: ").append(name).append("\n");
		for (TableIndex table : tables) {
			sb.append(table.toString()).append("\n");
		}
		return sb.toString();
	}

}
