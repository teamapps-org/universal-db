/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class RecordIndex extends PrimitiveEntryAtomicStore {

	private int maxSetId;
	private int numberOfSetIds;

	public RecordIndex(File path, String name) {
		super(path, name);
		recalculateMaxSetIndex();
		recalculateNumberOfSetIds();
	}

	private void recalculateMaxSetIndex() {
		int maximumId = (int) (getTotalCapacity() * 8) - 1;
		int maxId = 0;
		for (int id = maximumId; id > 0; id--) {
			if (getBoolean(id)) {
				maxId = id;
				break;
			}
		}
		maxSetId = maxId;
	}

	private void recalculateNumberOfSetIds() {
		int count = 0;
		for (int id = 1; id <= maxSetId; id++) {
			if (getBoolean(id)) {
				count++;
			}
		}
		numberOfSetIds = count;
	}

	@Override
	public void setBoolean(int id, boolean value) {
		if (id > 0 && value != getBoolean(id)) {
			if (value) {
				numberOfSetIds++;
			} else {
				numberOfSetIds--;
			}
		}
		super.setBoolean(id, value);
		if (value) {
			if (id > maxSetId) {
				maxSetId = id;
			}
		} else {
			if (id == maxSetId) {
				recalculateMaxSetIndex();
			}
		}
	}

	public int createRecord() {
		maxSetId++;
		numberOfSetIds++;
		int id = maxSetId;
		super.setBoolean(id, true);
		return id;
	}

	public int getCount() {
		return numberOfSetIds;
	}

	public BitSet getBitSet() {
		BitSet bitSet = new BitSet(maxSetId);
		for (int i = 1; i <= maxSetId; i++) {
			if (getBoolean(i)) {
				bitSet.set(i);
			}
		}
		return bitSet;
	}

	public List<Integer> getRecords() {
		List<Integer> recordIds = new ArrayList<>();
		for (int i = 1; i <= maxSetId; i++) {
			if (getBoolean(i)) {
				recordIds.add(i);
			}
		}
		return recordIds;
	}

	public int getMaxId() {
		return maxSetId;
	}

	public int getNextAvailableId() {
		return maxSetId + 1;
	}
}
