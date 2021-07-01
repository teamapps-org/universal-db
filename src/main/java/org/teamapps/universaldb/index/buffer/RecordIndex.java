package org.teamapps.universaldb.index.buffer;

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
