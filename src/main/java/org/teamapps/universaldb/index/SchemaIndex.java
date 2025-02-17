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
package org.teamapps.universaldb.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.universaldb.index.filelegacy.FileStore;
import org.teamapps.universaldb.schema.Column;
import org.teamapps.universaldb.schema.Database;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.schema.Table;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.*;

public class SchemaIndex {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final Schema schema;
	private final File dataPath;
	private final File fullTextIndexPath;
	private final List<DatabaseIndex> databases;
	private FileStore fileStore;

	public SchemaIndex(Schema schema, File path) {
		this.schema = schema;
		this.dataPath = new File(path, "data");
		this.fullTextIndexPath = new File(path, "full-text-index");
		dataPath.mkdir();
		fullTextIndexPath.mkdir();
		databases = new ArrayList<>();
	}

	public File getDataPath() {
		return dataPath;
	}

	public File getFullTextIndexPath() {
		return fullTextIndexPath;
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

	public void merge(Schema schema, boolean checkFullTextIndex, UniversalDB universalDB) {
//		if (!this.schema.isCompatibleWith(schema)) {
//			throw new RuntimeException("Error: cannot merge incompatible schemas:" + this + " with " + schema);
//		}
//		Map<String, DatabaseIndex> databaseMap = databases.stream().collect(Collectors.toMap(DatabaseIndex::getName, db -> db));
//		for (Database database : schema.getDatabases()) {
//			DatabaseIndex localDatabase = databaseMap.get(database.getName());
//			if (localDatabase == null) {
//				localDatabase = new DatabaseIndex(this, database.getName());
//				databases.add(localDatabase);
//			}
//			if (localDatabase.getMappingId() == 0) {
//				localDatabase.setMappingId(database.getMappingId());
//			}
//			localDatabase.merge(database, checkFullTextIndex, universalDB);
//		}
//		for (Database database : schema.getDatabases()) {
//			for (Table table : database.getTables()) {
//				for (Column column : table.getColumns()) {
//					if (column.getReferencedTable() != null) {
//						FieldIndex fieldIndex = getColumn(column);
//						TableIndex referencedTable = getReferencedTable(column.getReferencedTable());
//						if (referencedTable == null) {
//							logger.warn("Missing referenced table:" + column.getReferencedTable().getFQN() + ", " + column.getReferencedTable().getReferencedTablePath());
//						}
//						FieldIndex backReference = null;
//						if (column.getBackReference() != null) {
//							backReference = referencedTable.getColumnIndex(column.getBackReference());
//						}
//						if (fieldIndex instanceof SingleReferenceIndex) {
//							SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) fieldIndex;
//							singleReferenceIndex.setReferencedTable(referencedTable, backReference, column.isCascadeDeleteReferences());
//						} else {
//							MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) fieldIndex;
//							multiReferenceIndex.setReferencedTable(referencedTable, backReference, column.isCascadeDeleteReferences());
//						}
//					}
//				}
//			}
//		}
//		this.schema.merge(schema);
	}

	public boolean checkModel() {
//		Set<Integer> mappingIds = new HashSet<>();
//		for (DatabaseIndex database : databases) {
//			if (database.getMappingId() == 0) {
//				logger.error("Missing mapping id:" + database.getFQN());
//				return false;
//			} else if (mappingIds.contains(database.getMappingId())) {
//				logger.error("Duplicate mapping id:" + database.getFQN());
//				return false;
//			}
//			mappingIds.add(database.getMappingId());
//			for (TableIndex table : database.getTables()) {
//				if (table.getMappingId() == 0) {
//					logger.error("Missing mapping id:" + table.getFQN());
//					return false;
//				} else if (mappingIds.contains(table.getMappingId())) {
//					logger.error("Duplicate mapping id:" + table.getFQN());
//					return false;
//				}
//				mappingIds.add(table.getMappingId());
//				for (FieldIndex fieldIndex : table.getFieldIndices()) {
//					if (fieldIndex.getMappingId() == 0) {
//						logger.error("Missing mapping id:" + fieldIndex.getFQN());
//						return false;
//					} else if (mappingIds.contains(fieldIndex.getMappingId())) {
//						logger.error("Duplicate mapping id:" + fieldIndex.getFQN());
//						return false;
//					}
//					mappingIds.add(fieldIndex.getMappingId());
//				}
//			}
//		}
		return true;
	}

	public Column getColumn(FieldIndex index) {
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

	public TableIndex getTable(Table table) {
		Database database = table.getDatabase();
		DatabaseIndex databaseIndex = getDatabase(database.getName());
		return databaseIndex.getTable(table.getName());
	}

	public TableIndex getReferencedTable(Table table) {
		if (table.getReferencedTablePath() != null) {
			return getTableByPath(table.getReferencedTablePath());
		} else {
			return getTable(table);
		}
	}

	public TableIndex getTableByPath(String path) {
		if (path == null || !path.contains(".")) {
			return null;
		}
		String[] parts = path.split("\\.");
		if (parts.length != 2) {
			return null;
		}
		DatabaseIndex database = getDatabase(parts[0]);
		if (database != null) {
			return database.getTable(parts[1]);
		}
		return null;
	}

	public FieldIndex getColumn(Column column) {
		Table table = column.getTable();
		TableIndex tableIndex = getTable(table);
		return tableIndex.getFieldIndex(column.getName());
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
