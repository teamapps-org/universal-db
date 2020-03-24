package org.teamapps.universaldb.distribute;

public enum TransactionMessageType {

	TRANSACTION,
	SCHEMA_UPDATE,
	SNAPSHOT_REQUEST,
	CHECKSUM_REQUEST,
}
