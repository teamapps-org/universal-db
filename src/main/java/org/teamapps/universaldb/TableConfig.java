/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
package org.teamapps.universaldb;

import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.schema.TableOption;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

public class TableConfig {

	public final static int CHECKPOINTS = 1;
	public final static int VERSIONING = 2;
	public final static int HIERARCHY = 3;
	public final static int TRACK_CREATION = 4;
	public final static int TRACK_MODIFICATION = 5;
	public final static int KEEP_DELETED = 6;

	private final BitSet bitSet;

	public static IndexType getIndexType(int id) {
		switch (id) {
			case CHECKPOINTS:
			case VERSIONING:
				return IndexType.LONG;
			default:
				return IndexType.INT;
		}
	}


	public static TableConfig parse(String tableDefinition) {
		if (!tableDefinition.contains("(")) {
			return new TableConfig();
		}
		String line = tableDefinition.substring(tableDefinition.lastIndexOf('(') + 1, tableDefinition.lastIndexOf(')'));
		String[] parts = line.split(", ");
		List<TableOption> tableOptions = new ArrayList<>();
		for (String part : parts) {
			tableOptions.add(TableOption.valueOf(part.trim()));
		}
		return create(tableOptions);
	}

	public static TableConfig create(List<TableOption> tableOptions) {
		TableConfig config = new TableConfig();
		for (TableOption option : tableOptions) {
			config.setOption(option.getId());
		}
		return config;
	}

	public static TableConfig create(TableOption ... options) {
		TableConfig config = new TableConfig();
		for (TableOption option : options) {
			config.setOption(option.getId());
		}
		return config;
	}

	public TableConfig() {
		this.bitSet = new BitSet();
	}

	public void merge(TableConfig config) {
		this.bitSet.or(config.bitSet);
	}

	public List<TableOption> getTableOptions() {
		List<TableOption> tableOptions = new ArrayList<>();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			TableOption tableOption = TableOption.getById(id);
			if (tableOption != null) {
				tableOptions.add(tableOption);
			}
		}
		return tableOptions;
	}

	public String writeConfig() {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		for (TableOption option : getTableOptions()) {
			if (sb.length() > 1) {
				sb.append(", ");
			}
			sb.append(option.name());
		}
		if (sb.length() == 1) {
			return "";
		}
		sb.append(")");
		return sb.toString();
	}

	public boolean isCheckpoints() {
		return getOption(CHECKPOINTS);
	}

	public boolean isVersioning() {
		return getOption(VERSIONING);
	}

	public boolean isHierarchy() {
		return getOption(HIERARCHY);
	}

	public boolean trackCreation() {
		return getOption(TRACK_CREATION);
	}

	public boolean trackModification() {
		return getOption(TRACK_MODIFICATION);
	}

	public boolean keepDeleted() {
		return getOption(KEEP_DELETED);
	}

	public void setOption(int id) {
		if (id > 30) {
			return;
		}
		bitSet.set(id);
	}

	public boolean getOption(int id) {
		return bitSet.get(id);
	}

	public IntStream getColumns() {
		return bitSet.stream();
	}

	@Override
	public String toString() {
		return bitSet.toString();
	}

}
