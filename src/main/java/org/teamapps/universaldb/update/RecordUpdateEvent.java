package org.teamapps.universaldb.update;

public class RecordUpdateEvent {

	private final int tableId;
	private final int recordId;
	private final int userId;
	private final RecordUpdateType type;

	public RecordUpdateEvent(int tableId, int recordId, int userId, RecordUpdateType type) {
		this.tableId = tableId;
		this.recordId = recordId;
		this.userId = userId;
		this.type = type;
	}

	public int getTableId() {
		return tableId;
	}

	public int getRecordId() {
		return recordId;
	}

	public int getUserId() {
		return userId;
	}

	public RecordUpdateType getType() {
		return type;
	}
}
