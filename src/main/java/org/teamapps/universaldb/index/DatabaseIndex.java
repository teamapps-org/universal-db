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

import org.teamapps.commons.util.collections.ByKeyComparisonResult;
import org.teamapps.commons.util.collections.CollectionUtil;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.universaldb.index.file.store.DatabaseFileStore;
import org.teamapps.universaldb.index.file.store.FileStore;
import org.teamapps.universaldb.index.file.store.LocalFileStore;
import org.teamapps.universaldb.model.DatabaseModel;
import org.teamapps.universaldb.model.ReferenceFieldModel;
import org.teamapps.universaldb.model.TableModel;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DatabaseIndex {

	private final String name;
	private final File dataPath;
	private final File fullTextIndexPath;
	private final List<TableIndex> tables;
	private final DatabaseFileStore databaseFileStore;
	private DatabaseModel databaseModel;


	public DatabaseIndex(String name, File dataPath, File fullTextIndexPath, DatabaseFileStore databaseFileStore) {
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

		ByKeyComparisonResult<TableIndex, TableModel, String> compareResult = CollectionUtil.compareByKey(tables, databaseModel.getTables(), TableIndex::getName, TableModel::getName, true);
		//unknown table indices for this model
		if (!compareResult.getAEntriesNotInB().isEmpty()) {
			throw new RuntimeException("Unknown table indices that are not within the model:" + compareResult.getAEntriesNotInB().stream().map(TableIndex::getName).collect(Collectors.joining(", ")));
		}

		//existing tables
		for (TableIndex tableIndex : compareResult.getAEntriesInB()) {
			TableModel tableModel = compareResult.getB(tableIndex);
			tableIndex.merge(tableModel);
		}

		//new tables
		for (TableModel tableModel : compareResult.getBEntriesNotInA()) {
			TableIndex tableIndex = new TableIndex(this, tableModel);
			tableIndex.merge(tableModel);
			addTable(tableIndex);
		}

		tables.stream().flatMap(tableIndex ->  tableIndex.getReferenceFields().stream()).forEach(index -> {
			ReferenceFieldModel referenceFieldModel = index.getReferenceFieldModel();
			TableIndex referencedTable = referenceFieldModel.getReferencedTable() != null ? getTable(referenceFieldModel.getReferencedTable().getName()) : null;
			FieldIndex reverseIndex = referenceFieldModel.getReverseReferenceField() != null ? referencedTable.getFieldIndex(referenceFieldModel.getReverseReferenceField().getName()) : null;
			index.setReferencedTable(referencedTable, reverseIndex, referenceFieldModel.isCascadeDelete());
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
