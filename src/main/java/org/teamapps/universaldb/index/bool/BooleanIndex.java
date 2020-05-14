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
package org.teamapps.universaldb.index.bool;

import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.transaction.DataType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;

public class BooleanIndex extends AbstractBufferIndex<Boolean, BooleanFilter> {

	public static final int ENTRY_SIZE = 1;
	public static final byte[] BIT_MASKS = new byte[8];

	static {
		BIT_MASKS[0] = 0b00000001;
		BIT_MASKS[1] = 0b00000010;
		BIT_MASKS[2] = 0b00000100;
		BIT_MASKS[3] = 0b00001000;
		BIT_MASKS[4] = 0b00010000;
		BIT_MASKS[5] = 0b00100000;
		BIT_MASKS[6] = 0b01000000;
		BIT_MASKS[7] = (byte) 0b10000000;
	}

	private int maxSetId;
	private int numberOfSetIds;

	public BooleanIndex(String name, TableIndex tableIndex, ColumnType columnType) {
		super(name, tableIndex, columnType, FullTextIndexingOptions.NOT_INDEXED);
		recalculateMaxSetIndex();
		recalculateNumberOfSetIds();
	}

	private void recalculateMaxSetIndex() {
		int maximumId = getMaximumId() * 8;
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
	protected int getEntrySize() {
		return ENTRY_SIZE;
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
	public void setGenericValue(int id, Boolean value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, false);
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
		if (id > getMaximumId() * 8) {
			return false;
		}
		int index = getIndexForId(id / 8);
		int offset = getOffsetForIndex(index);

		int position = (id / 8) - offset;

		int bit = id % 8;
		byte b = getBuffer(index).getByte(position);
		boolean value = false;
		if ((b & BIT_MASKS[bit]) == BIT_MASKS[bit]) {
			value = true;
		}
		return value;
	}

	public void setValue(int id, boolean value) {
		ensureBufferSize((id / 8) + 1);
		int index = getIndexForId(id / 8);
		int offset = getOffsetForIndex(index);
		int position = (id / 8) - offset;

		if (id > 0 && value != getValue(id)) {
			if (value) {
				numberOfSetIds++;
			} else {
				numberOfSetIds--;
			}
		}

		int bit = id % 8;
		byte b = getBuffer(index).getByte(position);
		if (value) {
			b = (byte) (b | BIT_MASKS[bit]);
		} else {
			b = (byte) (b & ~BIT_MASKS[bit]);
		}
		getBuffer(index).putByte(position, b);
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

	@Override
	public void writeTransactionValue(Boolean value, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.BOOLEAN.getId());
		dataOutputStream.writeBoolean(value);
	}

	@Override
	public Boolean readTransactionValue(DataInputStream dataInputStream) throws IOException {
		return dataInputStream.readBoolean();
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale) {
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
