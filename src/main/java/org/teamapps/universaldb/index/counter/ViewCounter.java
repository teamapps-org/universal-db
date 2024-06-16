package org.teamapps.universaldb.index.counter;

import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.pojo.AbstractUdbEntity;
import org.teamapps.universaldb.record.EntityBuilder;

public interface ViewCounter {

	static ViewCounter getCounter(EntityBuilder<?> entityBuilder) {
		AbstractUdbEntity<?> entity = (AbstractUdbEntity<?>) entityBuilder;
		TableIndex tableIndex = entity.getTableIndex();
		return tableIndex.getDatabaseIndex().getUniversalDB().getOrCreateViewCounter(tableIndex);
	}

	int getViews(int id);

	void addView(int id);
}
