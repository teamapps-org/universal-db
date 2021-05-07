/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
package org.teamapps.universaldb.index.reference.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.PrimitiveIterator;

public class ReferenceIteratorValue implements MultiReferenceValue {

	private final List<Integer> recordIds;

	public ReferenceIteratorValue(List<Integer> recordIds) {
		this.recordIds = recordIds;
	}

	@Override
	public MultiReferenceValueType getType() {
		return MultiReferenceValueType.REFERENCE_ITERATOR;
	}

	@Override
	public void writeValues(DataOutputStream dataOutputStream) throws IOException {

	}

	@Override
	public void readValues(DataInputStream dataInputStream) throws IOException {

	}

	public int getReferencesCount() {
		return recordIds.size();
	}

	public List<Integer> getAsList() {
		return recordIds;
	}

	public BitSet getAsBitSet() {
		BitSet bitSet = new BitSet();
		if (recordIds != null) {
			recordIds.forEach(bitSet::set);
		}
		return bitSet;
	}
}
