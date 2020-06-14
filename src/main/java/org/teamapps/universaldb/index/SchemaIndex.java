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
package org.teamapps.universaldb.index;

import org.teamapps.universaldb.index.file.FileStore;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.schema.Column;
import org.teamapps.universaldb.schema.Database;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.schema.Table;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SchemaIndex {

	private final Schema schema;
	private final File path;
	private final List<DatabaseIndex> databases;
	private FileStore fileStore;

	public SchemaIndex(Schema schema, File path) {
		this.schema = schema;
		this.path = path;
		databases = new ArrayList<>();
	}

	public File getPath() {
		return path;
	}

	public void addDataBase(DatabaseIndex database) {
		databases.add(database);
	}

	public List<DatabaseIndex> getDatabases() {
		return databases;
	}

	public DatabaseIndex getDatabase(String name) {
		return databases.stream().filter(db -> db.getName().equals(name)).findAny().orElse(null);
	}

	public Schema getSchema() {
		return schema;
	}

	public FileStore getFileStore() {
		return fileStore;
	}

	public void setFileStore(FileStore fileStore) {
		this.fileStore = fileStore;
	}

	public void merge(Schema schema) {
		if (!this.schema.isCompatibleWith(schema)) {
			throw new RuntimeException("Error: cannot merge incompatible schemas:" + this + " with " + schema);
		}
		Map<String, DatabaseIndex> databaseMap = databases.stream().collect(Collectors.toMap(DatabaseIndex::getName, db -> db));
		for (Database database : schema.getDatabases()) {
			DatabaseIndex localDatabase = databaseMap.get(database.getName());
			if (localDatabase == null) {
				localDatabase = new DatabaseIndex(this, database.getName());
				databases.add(localDatabase);
			}
			if (localDatabase.getMappingId() == 0) {
				localDatabase.setMappingId(database.getMappingId());
			}
			localDatabase.merge(database);
		}
		for (Database database : schema.getDatabases()) {
			for (Table table : database.getTables()) {
				for (Column column : table.getColumns()) {
					if (column.getReferencedTable() != null) {
						ColumnIndex columnIndex = getColumn(column);
						TableIndex referencedTable = getTable(column.getReferencedTable());
						ColumnIndex backReference = null;
						if (column.getBackReference() != null) {
							backReference = referencedTable.getColumnIndex(column.getBackReference());
						}
						if (columnIndex instanceof SingleReferenceIndex) {
							SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) columnIndex;
							singleReferenceIndex.setReferencedTable(referencedTable, backReference, column.isCascadeDeleteReferences());
						} else {
							MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) columnIndex;
							multiReferenceIndex.setReferencedTable(referencedTable, backReference, column.isCascadeDeleteReferences());
						}
					}
				}
			}
		}
		this.schema.merge(schema);
	}

	public Column getColumn(ColumnIndex index) {
		String fqn = index.getFQN();
		for (Database database : schema.getDatabases()) {
			for (Table table : database.getTables()) {
				for (Column column : table.getColumns()) {
					if ((database.getName() + "." + table.getName() + "." + column.getName()).equals(fqn)) {
						return column;
					}
				}
			}
		}
		return null;
	}

	private TableIndex getTable(Table table) {
		Database database = table.getDatabase();
		DatabaseIndex databaseIndex = getDatabase(database.getName());
		return databaseIndex.getTable(table.getName());
	}

	private ColumnIndex getColumn(Column column) {
		Table table = column.getTable();
		TableIndex tableIndex = getTable(table);
		return tableIndex.getColumnIndex(column.getName());
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Schema").append("\n");
		for (DatabaseIndex database : databases) {
			sb.append(database.toString()).append("\n");
		}
		return sb.toString();
	}

}
