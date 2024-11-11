/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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

import org.teamapps.universaldb.model.BaseFieldModel;
import org.teamapps.universaldb.model.FieldModel;
import org.teamapps.universaldb.model.FieldType;

import java.io.File;
import java.util.BitSet;

public abstract class AbstractIndex<TYPE, FILTER> implements FieldIndex<TYPE, FILTER> {

	private final FieldModel fieldModel;
	private final TableIndex table;

	public AbstractIndex(FieldModel fieldModel, TableIndex table) {
		this.table = table;
		this.fieldModel = fieldModel;
		new IndexMetaData(table.getDataPath(), fieldModel.getName(), getFQN(), fieldModel.getFieldType().getId(), fieldModel.getFieldId());
	}

	@Override
	public String getName() {
		return fieldModel.getName();
	}

	@Override
	public String getFQN() {
		return table.getFQN() + "." + getName();
	}

	@Override
	public TableIndex getTable() {
		return table;
	}

	@Override
	public FieldType getFieldType() {
		return fieldModel.getFieldType();
	}

	@Override
	public FieldModel getFieldModel() {
		return fieldModel;
	}

	@Override
	public int getMappingId() {
		return fieldModel.getFieldId();
	}

	@Override
	public String toString() {
		return "field: " + getName() + ", type:" + getType().name() + ", id:" + getMappingId();
	}

	public static BitSet negateInput(BitSet records, BitSet input) {
		BitSet data = (BitSet) records.clone();
		data.andNot(input);
		return data;
	}

}
