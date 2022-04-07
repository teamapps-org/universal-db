package org.teamapps.universaldb.index.transaction;

public enum TransactionType {

	MODEL_UPDATE(1),
	DATA_UPDATE(2),

	;
	private final int id;

	TransactionType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static TransactionType getById(int id) {
		switch (id) {
			case 1: return MODEL_UPDATE;
			case 2: return DATA_UPDATE;
		}
		return null;
	}
}
