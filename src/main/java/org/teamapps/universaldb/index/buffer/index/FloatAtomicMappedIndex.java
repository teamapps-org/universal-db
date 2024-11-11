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

public class FloatAtomicMappedIndex {

	private final PrimitiveEntryAtomicStore atomicStore;

	public FloatAtomicMappedIndex(File path, String name) {
		atomicStore = new PrimitiveEntryAtomicStore(path, name);
	}

	public float getValue(int id) {
		return atomicStore.getFloat(id);
	}

	public void setValue(int id, float value) {
		atomicStore.setFloat(id, value);
	}

	public boolean isEmpty(int id) {
		return getValue(id) != 0;
	}

	public int getMaximumId() {
		return atomicStore.getMaximumId(4);
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

	public BitSet filterEquals(float value, BitSet bitSet) {
		return filterEquals(value, bitSet.stream());
	}

	public BitSet filterEquals(float value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getFloat(id) == value).forEach(result::set);
		return result;
	}

	public BitSet filterNotEquals(float value, BitSet bitSet) {
		return filterNotEquals(value, bitSet.stream());
	}

	public BitSet filterNotEquals(float value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getFloat(id) != value).forEach(result::set);
		return result;
	}

	public BitSet filterGreater(float value, BitSet bitSet) {
		return filterGreater(value, bitSet.stream());
	}

	public BitSet filterGreater(float value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getFloat(id) > value).forEach(result::set);
		return result;
	}

	public BitSet filterSmaller(float value, BitSet bitSet) {
		return filterSmaller(value, bitSet.stream());
	}

	public BitSet filterSmaller(float value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getFloat(id) < value).forEach(result::set);
		return result;
	}

	public BitSet filterBetween(float startValue, float endValue, BitSet bitSet) {
		return filterBetween(startValue, endValue, bitSet.stream());
	}

	public BitSet filterBetween(float startValue, float endValue, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> {
			float v = atomicStore.getFloat(id);
			return v > startValue && v < endValue;
		}).forEach(result::set);
		return result;
	}

	public BitSet filterContains(Set<Float> valueSet, BitSet bitSet) {
		return filterContains(valueSet, bitSet.stream());
	}

	public BitSet filterContains(Set<Float> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> valueSet.contains(atomicStore.getFloat(id))).forEach(result::set);
		return result;
	}

	public BitSet filterContainsNot(Set<Float> valueSet, BitSet bitSet) {
		return filterContainsNot(valueSet, bitSet.stream());
	}

	public BitSet filterContainsNot(Set<Float> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> !valueSet.contains(atomicStore.getFloat(id))).forEach(result::set);
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
