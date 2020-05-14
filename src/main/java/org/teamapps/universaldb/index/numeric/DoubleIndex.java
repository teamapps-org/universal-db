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
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.*;

public class DoubleIndex extends AbstractBufferIndex<Double, NumericFilter> implements NumericIndex {

	public static final int ENTRY_SIZE = 8;

	public DoubleIndex(String name, TableIndex table, ColumnType columnType) {
		super(name, table, columnType, FullTextIndexingOptions.NOT_INDEXED);
	}

	@Override
	protected int getEntrySize() {
		return ENTRY_SIZE;
	}

	@Override
	public IndexType getType() {
		return IndexType.DOUBLE;
	}

	@Override
	public Double getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public void setGenericValue(int id, Double value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, 0);
	}

	public double getValue(int id) {
		if (id > getMaximumId()) {
			return 0;
		}
		int index = getIndexForId(id);
		int offset = getOffsetForIndex(index);

		int position = (id - offset) * ENTRY_SIZE;
		return getBuffer(index).getDouble(position, ByteOrder.LITTLE_ENDIAN);
	}

	public void setValue(int id, double value) {
		ensureBufferSize(id);
		int index = getIndexForId(id);
		int offset = getOffsetForIndex(index);
		int position = (id - offset) * ENTRY_SIZE;
		getBuffer(index).putDouble(position, value, ByteOrder.LITTLE_ENDIAN);
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale) {
		int order = ascending ? 1 : -1;
		sortEntries.sort((o1, o2) -> {
			double value1 = getValue(o1.getLeafId());
			double value2 = getValue(o2.getLeafId());
			return Double.compare(value1, value2) * order;
		});
		return sortEntries;
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			double value = getValue(id);
			dataOutputStream.writeInt(id);
			dataOutputStream.writeDouble(value);
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			double value = dataInputStream.readDouble();
			setValue(id, value);
		} catch (EOFException ignore) {}
	}

	@Override
	public BitSet filter(BitSet records, NumericFilter numericFilter) {
		Set<Double> set = new HashSet<>();
		if (numericFilter.getValues() != null) {
			for (Number value : numericFilter.getValues()) {
				set.add(value.doubleValue());
			}
		}
		switch (numericFilter.getFilterType()) {
			case EQUALS:
				return filterEquals(records, numericFilter.getValue1().doubleValue());
			case NOT_EQUALS:
				return filterNotEquals(records, numericFilter.getValue1().doubleValue());
			case GREATER:
				return filterGreater(records, numericFilter.getValue1().doubleValue());
			case GREATER_EQUALS:
				return filterGreaterOrEquals(records, numericFilter.getValue1().doubleValue());
			case SMALLER:
				return filterSmaller(records, numericFilter.getValue1().doubleValue());
			case SMALLER_EQUALS:
				return filterSmallerOrEquals(records, numericFilter.getValue1().doubleValue());
			case BETWEEN:
				return filterBetween(records, numericFilter.getValue1().doubleValue(), numericFilter.getValue2().doubleValue());
			case BETWEEN_EXCLUSIVE:
				return filterBetweenExclusive(records, numericFilter.getValue1().doubleValue(), numericFilter.getValue2().doubleValue());
			case CONTAINS:
				return filterContains(records, set);
			case CONTAINS_NOT:
				return filterContainsNot(records, set);
		}
		return null;
	}

	@Override
	public void writeTransactionValue(Double value, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.DOUBLE.getId());
		dataOutputStream.writeDouble(value);
	}

	@Override
	public Double readTransactionValue(DataInputStream dataInputStream) throws IOException {
		return dataInputStream.readDouble();
	}

	public BitSet filterEquals(BitSet bitSet, double compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (value == compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterNotEquals(BitSet bitSet, double compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (value != compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterGreater(BitSet bitSet, double compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (value > compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterGreaterOrEquals(BitSet bitSet, double compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (value >= compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterSmaller(BitSet bitSet, double compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (value < compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterSmallerOrEquals(BitSet bitSet, double compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (value <= compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterBetween(BitSet bitSet, double start, double end) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (value >= start && value <= end) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterBetweenExclusive(BitSet bitSet, double start, double end) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (value > start && value < end) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContains(BitSet bitSet, Set<Double> set) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (set.contains(value)) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContainsNot(BitSet bitSet, Set<Double> set) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			double value = getValue(id);
			if (!set.contains(value)) {
				result.set(id);
			}
		}
		return result;
	}

}
