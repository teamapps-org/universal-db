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
package org.teamapps.universaldb.index.buffer.index;

import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;

import java.io.File;
import java.util.BitSet;
import java.util.Set;
import java.util.stream.IntStream;

public class ShortAtomicMappedIndex {

	private final PrimitiveEntryAtomicStore atomicStore;

	public ShortAtomicMappedIndex(File path, String name) {
		atomicStore = new PrimitiveEntryAtomicStore(path, name);
	}

	public short getValue(int id) {
		return atomicStore.getShort(id);
	}

	public void setValue(int id, short value) {
		atomicStore.setShort(id, value);
	}

	public boolean isEmpty(int id) {
		return getValue(id) != 0;
	}

	public int getMaximumId() {
		return atomicStore.getMaximumId(2);
	}

	public int getLastNonEmptyIndex() {
		int maximumId = getMaximumId();
		for (int i = maximumId; i > 0; i--) {
			if (!isEmpty(i)) {
				return i;
			}
		}
		return -1;
	}

	public IntStream getIndexStream() {
		return IntStream.range(1, getMaximumId() + 1);
	}

	public BitSet filterEquals(short value, BitSet bitSet) {
		return filterEquals(value, bitSet.stream());
	}

	public BitSet filterEquals(short value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getShort(id) == value).forEach(result::set);
		return result;
	}

	public BitSet filterNotEquals(short value, BitSet bitSet) {
		return filterNotEquals(value, bitSet.stream());
	}

	public BitSet filterNotEquals(short value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getShort(id) != value).forEach(result::set);
		return result;
	}

	public BitSet filterGreater(short value, BitSet bitSet) {
		return filterGreater(value, bitSet.stream());
	}

	public BitSet filterGreater(short value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getShort(id) > value).forEach(result::set);
		return result;
	}

	public BitSet filterSmaller(short value, BitSet bitSet) {
		return filterSmaller(value, bitSet.stream());
	}

	public BitSet filterSmaller(short value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getShort(id) < value).forEach(result::set);
		return result;
	}

	public BitSet filterBetween(short startValue, short endValue, BitSet bitSet) {
		return filterBetween(startValue, endValue, bitSet.stream());
	}

	public BitSet filterBetween(short startValue, short endValue, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> {
			short v = atomicStore.getShort(id);
			return v > startValue && v < endValue;
		}).forEach(result::set);
		return result;
	}

	public BitSet filterContains(Set<Short> valueSet, BitSet bitSet) {
		return filterContains(valueSet, bitSet.stream());
	}

	public BitSet filterContains(Set<Short> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> valueSet.contains(atomicStore.getShort(id))).forEach(result::set);
		return result;
	}

	public BitSet filterContainsNot(Set<Short> valueSet, BitSet bitSet) {
		return filterContainsNot(valueSet, bitSet.stream());
	}

	public BitSet filterContainsNot(Set<Short> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> !valueSet.contains(atomicStore.getShort(id))).forEach(result::set);
		return result;
	}

	public void flush() {
		atomicStore.flush();
	}

	public void close() {
		atomicStore.close();
	}

	public void drop() {
		atomicStore.drop();
	}
}
