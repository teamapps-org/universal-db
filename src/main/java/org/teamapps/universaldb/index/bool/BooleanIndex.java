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
package org.teamapps.universaldb.index.bool;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.buffer.index.RecordIndex;
import org.teamapps.universaldb.model.FieldModel;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

public class BooleanIndex extends AbstractIndex<Boolean, BooleanFilter> {

	private RecordIndex recordIndex;

	public BooleanIndex(FieldModel fieldModel, TableIndex tableIndex) {
		super(fieldModel, tableIndex);
		recordIndex = new RecordIndex(tableIndex.getDataPath(), fieldModel.getName());
	}


	@Override
	public IndexType getType() {
		return IndexType.BOOLEAN;
	}

	@Override
	public Boolean getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public boolean isEmpty(int id) {
		return !getValue(id);
	}

	@Override
	public void setGenericValue(int id, Boolean value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, false);
	}


	@Override
	public void close() {
		recordIndex.close();
	}

	@Override
	public void drop() {
		recordIndex.drop();
	}

	@Override
	public BitSet filter(BitSet records, BooleanFilter booleanFilter) {
		if (booleanFilter.getFilterValue()) {
			return filterEquals(records, true);
		} else {
			return filterEquals(records, false);
		}
	}

	public boolean getValue(int id) {
		return recordIndex.getBoolean(id);
	}

	public void setValue(int id, boolean value) {
		recordIndex.setBoolean(id, value);
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext) {
		int order = ascending ? 1 : -1;
		sortEntries.sort((o1, o2) -> {
			boolean value1 = getValue(o1.getLeafId());
			boolean value2 = getValue(o2.getLeafId());
			return Boolean.compare(value1, value2) * order;
		});
		return sortEntries;
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			boolean value = getValue(id);
			dataOutputStream.writeInt(id);
			dataOutputStream.writeBoolean(value);
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			boolean value = dataInputStream.readBoolean();
			setValue(id, value);
		} catch (EOFException ignore) {}
	}

	public BitSet filterEquals(BitSet bitSet, boolean compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			boolean value = getValue(id);
			if (value == compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterNotEquals(BitSet bitSet, boolean compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			boolean value = getValue(id);
			if (value != compare) {
				result.set(id);
			}
		}
		return result;
	}

	public int getCount() {
		return recordIndex.getCount();
	}

	public BitSet getBitSet() {
		return recordIndex.getBitSet();
	}

	public List<Integer> getRecords() {
		return recordIndex.getRecords();
	}

	public int getMaxId() {
		return recordIndex.getMaxId();
	}

}
