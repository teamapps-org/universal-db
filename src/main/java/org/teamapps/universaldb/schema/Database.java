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
package org.teamapps.universaldb.schema;

import org.teamapps.universaldb.index.MappedObject;
import org.teamapps.universaldb.TableConfig;

import java.util.*;
import java.util.stream.Collectors;

public class Database implements MappedObject {

	private final Schema schema;
	private final String name;
	private final List<Table> tables = new ArrayList<>();
	private int mappingId;

	public Database(Schema schema, String name) {
		this.schema = schema;
		this.name = name;
	}

	public Table addTable(String name, TableOption ... options) {
		List<TableOption> tableOptions = options == null ? Collections.emptyList() : Arrays.asList(options);
		return addTable(name, tableOptions);
	}

	public Table addTable(String name, List<TableOption> tableOptions) {
		Schema.checkName(name);
		Table table = new Table(this, name, TableConfig.create(tableOptions));
		return addTable(table);
	}

	public Table addTable(Table table) {
		tables.add(table);
		return table;
	}

	public Table addView(String name, String referencedTablePath) {
		Table view = new Table(this, name, TableConfig.create(), true, referencedTablePath);
		return addTable(view);
	}

	public Table addView(String name, Table referencedTable) {
		Table view = new Table(this, name, TableConfig.create(), true, referencedTable.getFQN());
		return addTable(view);
	}

	public Schema getSchema() {
		return schema;
	}

	public String getName() {
		return name;
	}

	public List<Table> getAllTables() {
		return tables;
	}

	public List<Table> getTables() {
		return tables.stream()
				.filter(table -> !table.isView())
				.collect(Collectors.toList());
	}

	public Table getTable(String name) {
		return tables.stream()
				.filter(table -> !table.isView())
				.filter(table -> table.getName().equals(name))
				.findFirst()
				.orElse(null);
	}

	public List<Table> getViewTables() {
		return tables.stream()
				.filter(Table::isView)
				.collect(Collectors.toList());
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

	public String createDefinition(boolean ignoreMapping) {
		StringBuilder sb = new StringBuilder();
		sb.append(name).append(" as DATABASE").append(createMappingDefinition(mappingId, ignoreMapping)).append("\n");
		tables.forEach(table -> sb.append(table.createDefinition(ignoreMapping)));
		return sb.toString();
	}

	protected String createMappingDefinition(int mappingId, boolean ignoreMapping) {
		if (mappingId == 0) {
			return "";
		} else {
			return " [" + mappingId + "]";
		}
	}

	public boolean isCompatibleWith(Database database) {
		Map<String, Table> tableMap = tables.stream().collect(Collectors.toMap(Table::getName, table -> table));
		for (Table table : database.getTables()) {
			Table localTable = tableMap.get(table.getName());
			if (localTable != null) {
				if (localTable.getMappingId() > 0 && table.getMappingId() > 0 && localTable.getMappingId() != table.getMappingId()) {
					return false;
				}
				boolean compatibleWith = localTable.isCompatibleWith(table);
				if (!compatibleWith) {
					return false;
				}
			} else {
				if (table.getMappingId() != 0 && getSchema().getMappingIds().contains(table.getMappingId())) {
					return false;
				}
			}
		}
		return true;
	}

	public void merge(Database database) {
		if (!isCompatibleWith(database)) {
			throw new RuntimeException("Error: cannot merge incompatible databases:" + getName() + " with " + database.getName());
		}
		Map<String, Table> tableMap = tables.stream().collect(Collectors.toMap(Table::getName, table -> table));
		for (Table table : database.getTables()) {
			Table localTable = tableMap.get(table.getName());
			if (localTable == null) {
				addTable(table);
			} else {
				if (localTable.getMappingId() == 0) {
					localTable.setMappingId(table.getMappingId());
				}
				localTable.merge(table);
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Database: ").append(name).append("\n");
		for (Table table : tables) {
			sb.append(table.toString()).append("\n");
		}
		return sb.toString();
	}

}
