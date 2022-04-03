package org.teamapps.universaldb.index.transaction.resolved;

import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordType;

public enum ResolvedTransactionRecordType {

	CREATE(1),
	CREATE_WITH_ID(2),
	UPDATE(3),
	DELETE(4),
	RESTORE(5),
	ADD_CYCLIC_REFERENCE(6),
	REMOVE_CYCLIC_REFERENCE(7);


	;
	private final int id;

	ResolvedTransactionRecordType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static ResolvedTransactionRecordType getById(int id) {
		switch (id) {
			case 1: return CREATE;
			case 2: return CREATE_WITH_ID;
			case 3: return UPDATE;
			case 4: return DELETE;
			case 5: return RESTORE;
			case 6: return ADD_CYCLIC_REFERENCE;
			case 7 : return REMOVE_CYCLIC_REFERENCE;
		}
		return null;
	}

	public static ResolvedTransactionRecordType getByRequestType(TransactionRequestRecordType type) {
		switch (type) {
			case CREATE: return CREATE;
			case CREATE_WITH_ID: return CREATE_WITH_ID;
			case UPDATE: return UPDATE;
			case DELETE: return DELETE;
			case RESTORE: return RESTORE;
		}
		return null;
	}
}
