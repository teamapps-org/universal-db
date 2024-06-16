package org.teamapps.universaldb.index.counter;

import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;

public class ViewCounterImpl implements ViewCounter{

	private PrimitiveEntryAtomicStore atomicStore;

	public ViewCounterImpl(TableIndex tableIndex) {
		atomicStore = new PrimitiveEntryAtomicStore(tableIndex.getDataPath(), "viewCounter.vdx");
	}

	@Override
	public int getViews(int id) {
		return atomicStore.getInt(id);
	}

	@Override
	public void addView(int id) {
		atomicStore.setInt(id, getViews(id) + 1);
	}
}
