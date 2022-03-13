package org.teamapps.universaldb.index.versioning;

public enum RecordUpdateType {
	CREATE(1), UPDATE(2), DELETE(3), RESTORE(4), ADD_CYCLIC_REFERENCE(5), REMOVE_CYCLIC_REFERENCE(6);

	private final int id;

	RecordUpdateType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static RecordUpdateType getById(int id) {
		switch (id) {
			case 1: return CREATE;
			case 2: return UPDATE;
			case 3: return DELETE;
			case 4: return RESTORE;
			case 5: return ADD_CYCLIC_REFERENCE;
			case 6: return REMOVE_CYCLIC_REFERENCE;
		}
		return null;
	}
}
