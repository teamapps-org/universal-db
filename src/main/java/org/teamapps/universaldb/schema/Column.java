/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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

import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.MappedObject;
import org.teamapps.universaldb.schema.Table;

import java.util.List;
import java.util.stream.Collectors;

public class Column implements MappedObject {

	private final Table table;
	private final String name;
	private final ColumnType type;
	private final IndexType indexType;
	private int mappingId;

	private Table referencedTable;
	private String backReference;
	private List<String> enumValues;
	private boolean cascadeDeleteReferences;


	public Column(Table table, String name, ColumnType type) {
		this.table = table;
		this.name = name;
		this.type = type;
		this.indexType = type.getIndexType();
	}

	public Table getTable() {
		return table;
	}

	public String getName() {
		return name;
	}

	public ColumnType getType() {
		return type;
	}

	@Override
	public String getFQN() {
		return table.getFQN() + "." + name;
	}

	public int getMappingId() {
		return mappingId;
	}

	public void setMappingId(int mappingId) {
		this.mappingId = mappingId;
	}

	public Table getReferencedTable() {
		return referencedTable;
	}

	public void setReferencedTable(Table referencedTable) {
		this.referencedTable = referencedTable;
	}

	public String getBackReference() {
		return backReference;
	}

	public void setBackReference(String backReference) {
		this.backReference = backReference;
	}

	public List<String> getEnumValues() {
		return enumValues;
	}

	public void setEnumValues(List<String> enumValues) {
		this.enumValues = enumValues;
	}

	public boolean isCascadeDeleteReferences() {
		return cascadeDeleteReferences;
	}

	public void setCascadeDeleteReferences(boolean cascadeDeleteReferences) {
		this.cascadeDeleteReferences = cascadeDeleteReferences;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public String createDefinition() {
		StringBuilder sb = new StringBuilder();
		sb.append("\t\t").append(name).append(" as ").append(type.name());
		switch (type) {
			case SINGLE_REFERENCE:
			case MULTI_REFERENCE:
				sb.append(" ");
				sb.append(referencedTable.getDatabase().getName()).append(".").append(referencedTable.getName());
				sb.append(" BACKREF ");
				if (backReference == null) {
					sb.append("NONE");
				} else {
					sb.append(backReference);
				}
				if (cascadeDeleteReferences) {
					sb.append(" CASCADE DELETE REFERENCES");
				}
				break;
			case ENUM:
				sb.append(" VALUES (");
				sb.append(enumValues.stream().collect(Collectors.joining(", "))).append(")");
				break;
		}
		sb.append("\n");
		return sb.toString();
	}

	@Override
	public String toString() {
		return "column: " + name + ", type:" + getType().name() + ", id:" + mappingId;
	}
}
