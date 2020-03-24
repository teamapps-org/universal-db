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

import org.teamapps.universaldb.index.TableIndex;

import java.util.BitSet;

public class BitSetBooleanIndex extends BooleanIndex {

	private BitSet bitSet;

	public BitSetBooleanIndex(String name, TableIndex table) {
		super(name, table);
		bitSet = new BitSet();
		for (int i = 0; i <= getMaximumId() * 8; i++) {
			bitSet.set(i, super.getValue(i));
		}
	}

	public boolean getValue(int id) {
		return bitSet.get(id);
	}

	public void setValue(int id, boolean value) {
		super.setValue(id, value);
		bitSet.set(id, value);
	}

	public BitSet getBitSet() {
		return bitSet;
	}
	
	public BitSet filterEquals(BitSet bitSet, boolean compare) {
		BitSet newBitSet = (BitSet) bitSet.clone();
		if (compare) {
			newBitSet.and(this.bitSet);
		} else {
			newBitSet.andNot(this.bitSet);
		}
		return newBitSet;
	}

	public BitSet filterNotEquals(BitSet bitSet, boolean compare) {
		BitSet newBitSet = (BitSet) bitSet.clone();
		if (compare) {
			newBitSet.andNot(this.bitSet);
		} else {
			newBitSet.and(this.bitSet);
		}
		return newBitSet;
	}

}
