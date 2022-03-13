package org.teamapps.universaldb.index.versioning;

import org.teamapps.universaldb.index.IndexType;

public class RecordUpdateValue {

	private final int columnId;
	private final IndexType indexType;
	private final Object value;

	public RecordUpdateValue(int columnId, IndexType indexType, Object value) {
		this.columnId = columnId;
		this.indexType = indexType;
		this.value = value;
	}

	public int getColumnId() {
		return columnId;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public Object getValue() {
		return value;
	}
}
