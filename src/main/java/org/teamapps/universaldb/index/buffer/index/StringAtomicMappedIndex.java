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

import org.teamapps.universaldb.index.buffer.common.BlockEntryAtomicStore;

import java.io.File;
import java.util.BitSet;
import java.util.Set;
import java.util.stream.IntStream;

public class StringAtomicMappedIndex {

	private BlockEntryAtomicStore atomicStore;

	public StringAtomicMappedIndex(File path, String name) {
		this.atomicStore = new BlockEntryAtomicStore(path, name);
	}

	public String getValue(int id) {
		return atomicStore.getText(id);
	}

	public void setValue(int id, String value) {
		atomicStore.setText(id, value);
	}

	public void removeValue(int id) {
		atomicStore.removeText(id);
	}

	public boolean isEmpty(int id) {
		return atomicStore.isEmpty(id);
	}

	public int getMaximumId() {
		return atomicStore.getLastNonEmptyId();
	}

	public int getLastNonEmptyId() {
		return atomicStore.getLastNonEmptyId();
	}

	public IntStream getIndexStream() {
		return IntStream.range(1, getMaximumId() + 1);
	}

	public BitSet filterEquals(String value, BitSet bitSet) {
		return filterEquals(value, bitSet.stream());
	}

	public BitSet filterEquals(String value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> atomicStore.getText(id).equals(value)).forEach(result::set);
		return result;
	}

	public BitSet filterNotEquals(String value, BitSet bitSet) {
		return filterNotEquals(value, bitSet.stream());
	}

	public BitSet filterNotEquals(String value, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> !atomicStore.getText(id).equals(value)).forEach(result::set);
		return result;
	}

	public BitSet filterContains(Set<String> valueSet, BitSet bitSet) {
		return filterContains(valueSet, bitSet.stream());
	}

	public BitSet filterContains(Set<String> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> valueSet.contains(atomicStore.getText(id))).forEach(result::set);
		return result;
	}

	public BitSet filterContainsNot(Set<String> valueSet, BitSet bitSet) {
		return filterContainsNot(valueSet, bitSet.stream());
	}

	public BitSet filterContainsNot(Set<String> valueSet, IntStream idStream) {
		BitSet result = new BitSet();
		idStream.filter(id -> !valueSet.contains(atomicStore.getText(id))).forEach(result::set);
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
