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

import java.io.File;
import java.util.BitSet;

public abstract class AbstractIndex<TYPE, FILTER> implements ColumnIndex<TYPE, FILTER> {

	private final File dataPath;
	private final File fullTextIndexPath;
	private final String name;
	private final TableIndex table;
	private final ColumnType columnType;
	private final FullTextIndexingOptions fullTextIndexingOptions;
	private int mappingId;
	private final IndexMetaData indexMetaData;


	public AbstractIndex(String name, TableIndex table, ColumnType columnType, FullTextIndexingOptions fullTextIndexingOptions) {
		this.name = name;
		this.dataPath = table.getDataPath();
		this.fullTextIndexPath = table.getFullTextIndexPath();
		this.table = table;
		this.columnType = columnType;
		this.fullTextIndexingOptions = fullTextIndexingOptions;
		this.indexMetaData = new IndexMetaData(dataPath, name, getFQN(), columnType.ordinal() + 1);
		this.mappingId = indexMetaData.getMappingId();
	}

	public File getDataPath() {
		return dataPath;
	}

	public File getFullTextIndexPath() {
		return fullTextIndexPath;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String getFQN() {
		return table.getFQN() + "." + name;
	}

	@Override
	public TableIndex getTable() {
		return table;
	}

	@Override
	public ColumnType getColumnType() {
		return columnType;
	}

	@Override
	public FullTextIndexingOptions getFullTextIndexingOptions() {
		return fullTextIndexingOptions;
	}

	@Override
	public int getMappingId() {
		return mappingId;
	}

	@Override
	public void setMappingId(int id) {
		if (mappingId > 0 && mappingId != id) {
			throw new RuntimeException("Error mapping index with different id:" + mappingId + " -> " + id);
		}
		if (mappingId > 0) {
			return;
		}
		this.mappingId = id;
		this.indexMetaData.setMappingId(id);
	}

	@Override
	public String toString() {
		return "column: " + name + ", type:" + getType().name() + ", id:" + mappingId;
	}

	public static BitSet negateInput(BitSet records, BitSet input) {
		BitSet data = (BitSet) records.clone();
		data.andNot(input);
		return data;
	}

}
