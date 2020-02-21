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
package org.teamapps.universaldb.index.numeric;

import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.transaction.DataType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.*;

public class LongIndex extends AbstractBufferIndex<Long, NumericFilter> {

	public static final int ENTRY_SIZE = 8;

	public LongIndex(String name, TableIndex tableIndex) {
		super(name, tableIndex, FullTextIndexingOptions.NOT_INDEXED);
	}

	@Override
	protected int getEntrySize() {
		return ENTRY_SIZE;
	}

	@Override
	public IndexType getType() {
		return IndexType.LONG;
	}

	@Override
	public Long getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public void setGenericValue(int id, Long value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, 0);
	}

	public long getValue(int id) {
		if (id > getMaximumId()) {
			return 0;
		}
		int index = getIndexForId(id);
		int offset = getOffsetForIndex(index);

		int position = (id - offset) * ENTRY_SIZE;
		return getBuffer(index).getLong(position, ByteOrder.LITTLE_ENDIAN);
	}

	public void setValue(int id, long value) {
		ensureBufferSize(id);
		int index = getIndexForId(id);
		int offset = getOffsetForIndex(index);
		int position = (id - offset) * ENTRY_SIZE;
		getBuffer(index).putLong(position, value, ByteOrder.LITTLE_ENDIAN);
	}

	@Override
	public void writeTransactionValue(Long value, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.LONG.getId());
		dataOutputStream.writeLong(value);
	}

	@Override
	public Long readTransactionValue(DataInputStream dataInputStream) throws IOException {
		return dataInputStream.readLong();
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale) {
		int order = ascending ? 1 : -1;
		sortEntries.sort((o1, o2) -> {
			long value1 = getValue(o1.getLeafId());
			long value2 = getValue(o2.getLeafId());
			return Long.compare(value1, value2) * order;
		});
		return sortEntries;
	}

	@Override
	public BitSet filter(BitSet records, NumericFilter numericFilter) {
		Set<Long> set = new HashSet<>();
		if (numericFilter.getValues() != null) {
			for (Number value : numericFilter.getValues()) {
				set.add(value.longValue());
			}
		}
		switch (numericFilter.getFilterType()) {
			case EQUALS:
				return filterEquals(records, numericFilter.getValue1().longValue());
			case NOT_EQUALS:
				return filterNotEquals(records, numericFilter.getValue1().longValue());
			case GREATER:
				return filterGreater(records, numericFilter.getValue1().longValue());
			case GREATER_EQUALS:
				return filterGreaterOrEquals(records, numericFilter.getValue1().longValue());
			case SMALLER:
				return filterSmaller(records, numericFilter.getValue1().longValue());
			case SMALLER_EQUALS:
				return filterSmallerOrEquals(records, numericFilter.getValue1().longValue());
			case BETWEEN:
				return filterBetween(records, numericFilter.getValue1().longValue(), numericFilter.getValue2().longValue());
			case BETWEEN_EXCLUSIVE:
				return filterBetweenExclusive(records, numericFilter.getValue1().longValue(), numericFilter.getValue2().longValue());
			case CONTAINS:
				return filterContains(records, set);
			case CONTAINS_NOT:
				return filterContainsNot(records, set);
		}
		return null;
	}

	public BitSet filterEquals(BitSet bitSet, long compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (value == compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterNotEquals(BitSet bitSet, long compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (value != compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterGreater(BitSet bitSet, long compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (value > compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterGreaterOrEquals(BitSet bitSet, long compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (value >= compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterSmaller(BitSet bitSet, long compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (value < compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterSmallerOrEquals(BitSet bitSet, long compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (value <= compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterBetween(BitSet bitSet, long start, long end) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (value >= start && value <= end) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterBetweenExclusive(BitSet bitSet, long start, long end) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (value > start && value < end) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContains(BitSet bitSet, Set<Long> set) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (set.contains(value)) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContainsNot(BitSet bitSet, Set<Long> set) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long value = getValue(id);
			if (!set.contains(value)) {
				result.set(id);
			}
		}
		return result;
	}

}
