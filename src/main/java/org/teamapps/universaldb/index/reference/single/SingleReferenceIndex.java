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
package org.teamapps.universaldb.index.reference.single;

import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.numeric.NumericFilter;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.value.RecordReference;
import org.teamapps.universaldb.transaction.DataType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteOrder;
import java.util.*;

public class SingleReferenceIndex extends AbstractBufferIndex<RecordReference, NumericFilter> {
	public static final int ENTRY_SIZE = 4;

	private TableIndex referencedTable;
	private boolean cyclicReferences;
	private SingleReferenceIndex reverseSingleIndex;
	private MultiReferenceIndex reverseMultiIndex;


	public SingleReferenceIndex(String name, TableIndex tableIndex) {
		super(name, tableIndex, FullTextIndexingOptions.NOT_INDEXED);
	}

	public void setReferencedTable(TableIndex referencedTable, ColumnIndex reverseIndex) {
		this.referencedTable = referencedTable;
		if (reverseIndex != null) {
			if (reverseIndex instanceof SingleReferenceIndex) {
				reverseSingleIndex = (SingleReferenceIndex) reverseIndex;
			} else {
				reverseMultiIndex = (MultiReferenceIndex) reverseIndex;
			}
			cyclicReferences = true;
		}
	}

	@Override
	protected int getEntrySize() {
		return ENTRY_SIZE;
	}

	@Override
	public IndexType getType() {
		return IndexType.REFERENCE;
	}

	public TableIndex getReferencedTable() {
		return referencedTable;
	}

	@Override
	public RecordReference getGenericValue(int id) {
		int value = getValue(id);
		if (value == 0) {
			return null;
		} else {
			return new RecordReference(value, 0);
		}
	}

	@Override
	public void setGenericValue(int id, RecordReference value) {
		if (value == null) {
			setValue(id, 0);
		} else {
 			setValue(id, value.getRecordId());
		}
	}

	@Override
	public void removeValue(int id) {
		setValue(id, 0);
	}

	public int getValue(int id) {
		if (id > getMaximumId()) {
			return 0;
		}
		int index = getIndexForId(id);
		int offset = getOffsetForIndex(index);

		int position = (id - offset) * ENTRY_SIZE;
		return getBuffer(index).getInt(position, ByteOrder.LITTLE_ENDIAN);
	}

	public void setValue(int id, int value) {
		if (cyclicReferences) {
			setCyclicReferences(id, value);
		}
		setIndexValue(id, value);
	}

	private void setCyclicReferences(int id, int value) {
		int previousValue = getValue(id);
		if (previousValue != value) {
			if (reverseSingleIndex != null) {
				if (previousValue > 0) {
					int orphanedReference = reverseSingleIndex.getValue(previousValue);
					assert orphanedReference > 0;
					setIndexValue(orphanedReference, 0);
					reverseSingleIndex.setIndexValue(previousValue, 0);
				}
				if (value > 0) {
					reverseSingleIndex.setIndexValue(value, id);
				}
			} else {
				if (previousValue > 0) {
					reverseMultiIndex.removeReferences(previousValue, Collections.singletonList(id), true);
				}
				if (value > 0) {
					reverseMultiIndex.addReferences(value, Collections.singletonList(id), true);
				}
			}
		}
	}

	public void setIndexValue(int id, int value) {
		ensureBufferSize(id);
		int index = getIndexForId(id);
		int offset = getOffsetForIndex(index);
		int position = (id - offset) * ENTRY_SIZE;
		getBuffer(index).putInt(position, value, ByteOrder.LITTLE_ENDIAN);
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale) {
		int order = ascending ? 1 : -1;
		sortEntries.sort((o1, o2) -> {
			int value1 = getValue(o1.getLeafId());
			int value2 = getValue(o2.getLeafId());
			return Integer.compare(value1, value2) * order;
		});
		return sortEntries;
	}

	@Override
	public void writeTransactionValue(RecordReference value, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.INTEGER.getId());
		if (value == null) {
			dataOutputStream.writeInt(0);
		} else {
			dataOutputStream.writeInt(value.getRecordId());
			dataOutputStream.writeInt(value.getCorrelationId());
		}
	}

	@Override
	public RecordReference readTransactionValue(DataInputStream dataInputStream) throws IOException {
		int val1 = dataInputStream.readInt();
		if (val1 == 0) {
			return null;
		} else {
			int val2 = dataInputStream.readInt();
			return new RecordReference(val1, val2);
		}
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			int value = getValue(id);
			dataOutputStream.writeInt(id);
			dataOutputStream.writeInt(value);
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			int value = dataInputStream.readInt();
			setIndexValue(id, value);
		} catch (EOFException ignore) {}
	}

	@Override
	public BitSet filter(BitSet records, NumericFilter numericFilter) {
		Set<Integer> set = new HashSet<>();
		if (numericFilter.getValues() != null) {
			for (Number value : numericFilter.getValues()) {
				set.add(value.intValue());
			}
		}
		switch (numericFilter.getFilterType()) {
			case EQUALS:
				return filterEquals(records, numericFilter.getValue1().intValue());
			case NOT_EQUALS:
				return filterNotEquals(records, numericFilter.getValue1().intValue());
			case GREATER:
				return filterGreater(records, numericFilter.getValue1().intValue());
			case GREATER_EQUALS:
				return filterGreaterOrEquals(records, numericFilter.getValue1().intValue());
			case SMALLER:
				return filterSmaller(records, numericFilter.getValue1().intValue());
			case SMALLER_EQUALS:
				return filterSmallerOrEquals(records, numericFilter.getValue1().intValue());
			case BETWEEN:
				return filterBetween(records, numericFilter.getValue1().intValue(), numericFilter.getValue2().intValue());
			case BETWEEN_EXCLUSIVE:
				return filterBetweenExclusive(records, numericFilter.getValue1().intValue(), numericFilter.getValue2().intValue());
			case CONTAINS:
				return filterContains(records, set);
			case CONTAINS_NOT:
				return filterContainsNot(records, set);
		}
		return null;
	}

	public BitSet filterEquals(BitSet bitSet, int compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (value == compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterNotEquals(BitSet bitSet, int compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (value != compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterGreater(BitSet bitSet, int compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (value > compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterGreaterOrEquals(BitSet bitSet, int compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (value >= compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterSmaller(BitSet bitSet, int compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (value < compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterSmallerOrEquals(BitSet bitSet, int compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (value <= compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterBetween(BitSet bitSet, int start, int end) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (value >= start && value <= end) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterBetweenExclusive(BitSet bitSet, int start, int end) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (value > start && value < end) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContains(BitSet bitSet, Set<Integer> set) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (set.contains(value)) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContainsNot(BitSet bitSet, Set<Integer> set) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int value = getValue(id);
			if (!set.contains(value)) {
				result.set(id);
			}
		}
		return result;
	}
}
