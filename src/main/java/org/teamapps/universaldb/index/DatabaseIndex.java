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
package org.teamapps.universaldb.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.commons.util.collections.ByKeyComparisonResult;
import org.teamapps.commons.util.collections.CollectionUtil;
import org.teamapps.universaldb.DatabaseManager;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.universaldb.index.file.store.DatabaseFileStore;
import org.teamapps.universaldb.model.DatabaseModel;
import org.teamapps.universaldb.model.ReferenceFieldModel;
import org.teamapps.universaldb.model.TableModel;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseIndex {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final UniversalDB universalDB;
	private final String name;
	private final File dataPath;
	private final File fullTextIndexPath;
	private final List<TableIndex> tables;
	private final DatabaseFileStore databaseFileStore;
	private DatabaseModel databaseModel;


	public DatabaseIndex(UniversalDB universalDB, String name, File dataPath, File fullTextIndexPath, DatabaseFileStore databaseFileStore) {
		this.universalDB = universalDB;
		this.name = name;
		this.dataPath = dataPath;
		this.fullTextIndexPath = fullTextIndexPath;
		this.databaseFileStore = databaseFileStore;
		this.tables = new ArrayList<>();
	}

	public File getDataPath() {
		return dataPath;
	}

	public File getFullTextIndexPath() {
		return fullTextIndexPath;
	}

	public DatabaseFileStore getDatabaseFileStore() {
		return databaseFileStore;
	}

	public void installModel(DatabaseModel model, boolean checkFullTextIndex, UniversalDB universalDB) {
		this.databaseModel = model;

		ByKeyComparisonResult<TableIndex, TableModel, String> compareResult = CollectionUtil.compareByKey(tables, databaseModel.getLocalTables(), TableIndex::getName, TableModel::getName, true);
		//unknown table indices for this model
		if (!compareResult.getAEntriesNotInB().isEmpty()) {
			throw new RuntimeException("Unknown table indices that are not within the model:" + compareResult.getAEntriesNotInB().stream().map(TableIndex::getName).collect(Collectors.joining(", ")));
		}

		//existing tables
		for (TableIndex tableIndex : compareResult.getAEntriesInB()) {
			TableModel tableModel = compareResult.getB(tableIndex);
			tableIndex.installOrMerge(tableModel);
		}

		//new tables
		for (TableModel tableModel : compareResult.getBEntriesNotInA()) {
			TableIndex tableIndex = new TableIndex(this, tableModel);
			tableIndex.installOrMerge(tableModel);
			addTable(tableIndex);
		}

		tables.stream().flatMap(tableIndex ->  tableIndex.getReferenceFields().stream()).forEach(index -> {
			ReferenceFieldModel referenceFieldModel = index.getReferenceFieldModel();
			TableIndex referencedTable = referenceFieldModel.getReferencedTable() != null ? getTable(referenceFieldModel.getReferencedTable().getName()) : null;
			FieldIndex reverseIndex = referenceFieldModel.getReverseReferenceField() != null ? referencedTable.getFieldIndex(referenceFieldModel.getReverseReferenceField().getName()) : null;
			if (referencedTable == null && !referenceFieldModel.getReferencedTable().isRemoteTable()) {
				throw new RuntimeException("Error missing reference table:" + name + "." +  index.getName() + " -> " + index.getReferenceFieldModel().getReferencedTable().getName());
			}
			if (referencedTable != null) {
				index.setReferencedTable(referencedTable, reverseIndex, referenceFieldModel.isCascadeDelete());
			}
		});

		if (checkFullTextIndex) {
			for (TableIndex tableIndex : tables) {
				if (tableIndex.getTableModel().isVersioning()) {
					tableIndex.getRecordVersioningIndex().checkVersionIndex(universalDB);
				}
				tableIndex.checkFullTextIndex();
			}
		}
	}

	public void installRemoteReferencedTables(DatabaseManager databaseManager) {
		logger.info("Install remote tables for DB: " + name);
		tables.stream().filter(t -> !t.getTableModel().isRemoteTable()).flatMap(tableIndex -> tableIndex.getReferenceFields().stream()).filter(index -> index.getReferenceFieldModel().getReferencedTable().isRemoteTable()).forEach(index -> {
			TableModel referencedTable = index.getReferenceFieldModel().getReferencedTable();
			UniversalDB remoteDb = databaseManager.getDatabase(referencedTable.getRemoteDatabase());
			TableIndex remoteTableIndex = remoteDb.getDatabaseIndex().getTable(referencedTable.getRemoteTableName());
			if (index.getReferencedTable() == null) {
				logger.info("Install remote reference table: " + name + "." + index.getTable().getName()  + index.getName() + " -> " + remoteDb.getName() + "." +  remoteTableIndex.getName());
				index.setReferencedTable(remoteTableIndex, null, false);
			} else {
				logger.info("Remote reference table already installed!: " + name + "." + index.getTable().getName() + "." +  index.getName() + " -> " + remoteDb.getName() + "." + remoteTableIndex.getName());
			}
		});
	}

	public String getName() {
		return name;
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

	public DatabaseModel getDatabaseModel() {
		return databaseModel;
	}

	public UniversalDB getUniversalDB() {
		return universalDB;
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
