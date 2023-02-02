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
import org.teamapps.universaldb.index.buffer.PrimitiveEntryAtomicStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;

public class BooleanIndex extends AbstractIndex<Boolean, BooleanFilter> {

	private PrimitiveEntryAtomicStore atomicStore;

	private int maxSetId;
	private int numberOfSetIds;

	public BooleanIndex(String name, TableIndex tableIndex, ColumnType columnType) {
		super(name, tableIndex, columnType, FullTextIndexingOptions.NOT_INDEXED);
		atomicStore = new PrimitiveEntryAtomicStore(tableIndex.getDataPath(), name);
		recalculateMaxSetIndex();
		recalculateNumberOfSetIds();
	}

	private void recalculateMaxSetIndex() {
		int maximumId = (int) (atomicStore.getTotalCapacity() * 8) - 1;
		int maxId = 0;
		for (int id = maximumId; id > 0; id--) {
			if (getValue(id)) {
				maxId = id;
				break;
			}
		}
		maxSetId = maxId;
	}

	private void recalculateNumberOfSetIds() {
		int count = 0;
		for (int id = 1; id <= maxSetId; id++) {
			if (getValue(id)) {
				count++;
			}
		}
		numberOfSetIds = count;
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
		atomicStore.close();
	}

	@Override
	public void drop() {
		atomicStore.drop();
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
		return atomicStore.getBoolean(id);
	}

	public void setValue(int id, boolean value) {
		if (id > 0 && value != getValue(id)) {
			if (value) {
				numberOfSetIds++;
			} else {
				numberOfSetIds--;
			}
		}
		atomicStore.setBoolean(id, value);
		if (value) {
			if (id > maxSetId) {
				maxSetId = id;
			}
		} else {
			if (id == maxSetId) {
				recalculateMaxSetIndex();
			}
		}
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
		return numberOfSetIds;
	}

	public BitSet getBitSet() {
		BitSet bitSet = new BitSet(maxSetId);
		for (int i = 1; i <= maxSetId; i++) {
			if (getValue(i)) {
				bitSet.set(i);
			}
		}
		return bitSet;
	}

	public int getMaxId() {
		return maxSetId;
	}

	public int getNextId() {
		return maxSetId + 1;
	}

}
