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
package org.teamapps.universaldb.index.numeric;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;
import org.teamapps.universaldb.model.FieldModel;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.*;

public class FloatIndex extends AbstractIndex<Float, NumericFilter> implements NumericIndex {

	private PrimitiveEntryAtomicStore atomicStore;

	public FloatIndex(FieldModel fieldModel, TableIndex tableIndex) {
		super(fieldModel, tableIndex);
		atomicStore = new PrimitiveEntryAtomicStore(tableIndex.getDataPath(), fieldModel.getName());
	}

	@Override
	public IndexType getType() {
		return IndexType.FLOAT;
	}

	@Override
	public Float getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public void setGenericValue(int id, Float value) {
		setValue(id, value);
	}

	@Override
	public boolean isEmpty(int id) {
		return getValue(id) == 0;
	}

	@Override
	public void removeValue(int id) {
		setValue(id, 0);
	}

	public float getValue(int id) {
		return atomicStore.getFloat(id);
	}

	public void setValue(int id, float value) {
		atomicStore.setFloat(id, value);
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext) {
		int order = ascending ? 1 : -1;
		sortEntries.sort((o1, o2) -> {
			float value1 = getValue(o1.getLeafId());
			float value2 = getValue(o2.getLeafId());
			return Float.compare(value1, value2) * order;
		});
		return sortEntries;
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			float value = getValue(id);
			dataOutputStream.writeInt(id);
			dataOutputStream.writeFloat(value);
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			float value = dataInputStream.readFloat();
			setValue(id, value);
		} catch (EOFException ignore) {}
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
	public BitSet filter(BitSet records, NumericFilter numericFilter) {
		Set<Float> set = new HashSet<>();
		if (numericFilter.getValues() != null) {
			for (Number value : numericFilter.getValues()) {
				set.add(value.floatValue());
			}
		}
		switch (numericFilter.getFilterType()) {
			case EQUALS:
				return filterEquals(records, numericFilter.getValue1().floatValue());
			case NOT_EQUALS:
				return filterNotEquals(records, numericFilter.getValue1().floatValue());
			case GREATER:
				return filterGreater(records, numericFilter.getValue1().floatValue());
			case GREATER_EQUALS:
				return filterGreaterOrEquals(records, numericFilter.getValue1().floatValue());
			case SMALLER:
				return filterSmaller(records, numericFilter.getValue1().floatValue());
			case SMALLER_EQUALS:
				return filterSmallerOrEquals(records, numericFilter.getValue1().floatValue());
			case BETWEEN:
				return filterBetween(records, numericFilter.getValue1().floatValue(), numericFilter.getValue2().floatValue());
			case BETWEEN_EXCLUSIVE:
				return filterBetweenExclusive(records, numericFilter.getValue1().floatValue(), numericFilter.getValue2().floatValue());
			case CONTAINS:
				return filterContains(records, set);
			case CONTAINS_NOT:
				return filterContainsNot(records, set);
		}
		return null;
	}
	
	public BitSet filterEquals(BitSet bitSet, float compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (value == compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterNotEquals(BitSet bitSet, float compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (value != compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterGreater(BitSet bitSet, float compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (value > compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterGreaterOrEquals(BitSet bitSet, float compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (value >= compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterSmaller(BitSet bitSet, float compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (value < compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterSmallerOrEquals(BitSet bitSet, float compare) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (value <= compare) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterBetween(BitSet bitSet, float start, float end) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (value >= start && value <= end) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterBetweenExclusive(BitSet bitSet, float start, float end) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (value > start && value < end) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContains(BitSet bitSet, Set<Float> set) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (set.contains(value)) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContainsNot(BitSet bitSet, Set<Float> set) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			float value = getValue(id);
			if (!set.contains(value)) {
				result.set(id);
			}
		}
		return result;
	}

}
