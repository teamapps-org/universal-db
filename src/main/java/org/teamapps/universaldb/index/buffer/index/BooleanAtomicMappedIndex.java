/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
import java.util.stream.IntStream;

public class BooleanAtomicMappedIndex {

	private final PrimitiveEntryAtomicStore atomicStore;

	public BooleanAtomicMappedIndex(File path, String name) {
		atomicStore = new PrimitiveEntryAtomicStore(path, name);
	}

	public boolean getValue(int id) {
		return atomicStore.getBoolean(id);
	}

	public void setValue(int id, boolean value) {
		atomicStore.setBoolean(id, value);
	}

	public boolean isEmpty(int id) {
		return getValue(id);
	}

	public int getMaximumId() {
		return atomicStore.getMaximumId(1) * 8;
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

	public BitSet filterEquals(boolean value, BitSet bitSet) {
		return filterEquals(value, bitSet.stream());
	}

	public BitSet filterEquals(boolean value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getBoolean(id) == value).forEach(result::set);
		return result;
	}

	public BitSet filterNotEquals(boolean value, BitSet bitSet) {
		return filterNotEquals(value, bitSet.stream());
	}

	public BitSet filterNotEquals(boolean value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getBoolean(id) != value).forEach(result::set);
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
