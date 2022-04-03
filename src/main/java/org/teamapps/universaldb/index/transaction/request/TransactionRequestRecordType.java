package org.teamapps.universaldb.index.transaction.request;

public enum TransactionRequestRecordType {

	CREATE(1),
	CREATE_WITH_ID(2),
	UPDATE(3),
	DELETE(4),
	RESTORE(5);

	private final int id;

	TransactionRequestRecordType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static TransactionRequestRecordType getById(int id) {
		switch (id) {
			case 1: return CREATE;
			case 2: return CREATE_WITH_ID;
			case 3: return UPDATE;
			case 4: return DELETE;
			case 5: return RESTORE;
		}
		return null;
	}
}
