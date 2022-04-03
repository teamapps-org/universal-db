package org.teamapps.universaldb.index.reference.value;

public enum ResolvedMultiReferenceType {
	REMOVE_ALL_REFERENCES(1),
	SET_REFERENCES(2),
	ADD_REMOVE_REFERENCES(3);

	private final int id;

	ResolvedMultiReferenceType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static ResolvedMultiReferenceType getById(int id) {
		switch (id) {
			case 1: return REMOVE_ALL_REFERENCES;
			case 2: return SET_REFERENCES;
			case 3: return ADD_REMOVE_REFERENCES;
		}
		return null;
	}
}
