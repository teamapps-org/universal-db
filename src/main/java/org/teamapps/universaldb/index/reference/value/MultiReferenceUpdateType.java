package org.teamapps.universaldb.index.reference.value;

@Deprecated
public enum MultiReferenceUpdateType {

	SET(1), ADD(2), REMOVE(3), REMOVE_ALL(4);

	private final int id;

	MultiReferenceUpdateType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static MultiReferenceUpdateType getById(int id) {
		switch (id) {
			case 1: return SET;
			case 2: return ADD;
			case 3: return REMOVE;
			case 4: return REMOVE_ALL;
		}
		return null;
	}
}
