package org.teamapps.universaldb;

import org.agrona.concurrent.AtomicBuffer;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.transaction.TransactionIdProvider;
import org.teamapps.universaldb.util.MappedStoreUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SchemaStats implements TransactionIdProvider {

	private static final int FIRST_SYSTEM_START_TIMESTAMP_POS = 0;
	private static final int SYSTEM_START_TIMESTAMP_POS = 8;
	private static final int SHUTDOWN_TIMESTAMP_POS = 16;
	private static final int LAST_TRANSACTION_ID_POS = 24;
	private static final int LAST_TRANSACTION_TIMESTAMP_POS = 32;
	private static final int START_UP_COUNT_POS = 40;

	private static final int CLIENT_ID_POS = 100;
	private static final int GROUP_ID_POS = 200;
	private static final int SCHEMA_POS = 300;

	private final File path;
	private AtomicBuffer buffer;
	private Schema schema;
	private final String clientId;
	private final String groupId;
	private long startupCount;

	public SchemaStats(File path) {
		this.path = path;
		File file = new File(this.path, "database.stat");
		buffer = MappedStoreUtil.createAtomicBuffer(file, 500_000);
		if (readLong(FIRST_SYSTEM_START_TIMESTAMP_POS) == 0) {
			writeLong(FIRST_SYSTEM_START_TIMESTAMP_POS, System.currentTimeMillis());
		}
		writeLong(SYSTEM_START_TIMESTAMP_POS, System.currentTimeMillis());
		if (readInt(CLIENT_ID_POS) == 0) {
			clientId = UUID.randomUUID().toString().replace("-", "");
			groupId = UUID.randomUUID().toString().replace("-", "");
			writeString(CLIENT_ID_POS, clientId);
			writeString(GROUP_ID_POS, groupId);
		} else {
			clientId = readString(CLIENT_ID_POS);
			groupId = readString(GROUP_ID_POS);
		}
		startupCount = readLong(START_UP_COUNT_POS) + 1;
		writeLong(START_UP_COUNT_POS, startupCount);
	}

	public Schema loadSchema() throws IOException {
		int len = buffer.getInt(SCHEMA_POS);
		if (len == 0) {
			return null;
		}
		byte[] bytes = new byte[len];
		buffer.getBytes(SCHEMA_POS + 4, bytes);
		return new Schema(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	public void saveSchema(Schema schema) throws IOException {
		byte[] bytes = schema.getSchemaData();
		buffer.putInt(SCHEMA_POS, bytes.length);
		buffer.putBytes(SCHEMA_POS + 4, bytes);
		this.schema = schema;
	}

	public Schema getSchema() {
		return schema;
	}

	public String getClientId() {
		return clientId;
	}

	public String getGroupId() {
		return groupId;
	}

	public long getStartupCount() {
		return startupCount;
	}

	public long getFirstSystemStartTimestamp() {
		return readLong(FIRST_SYSTEM_START_TIMESTAMP_POS);
	}

	public long getLastTransactionId() {
		return readLong(LAST_TRANSACTION_ID_POS);
	}

	public long setNextTransactionId() {
		long transactionId = getLastTransactionId();
		transactionId++;
		writeLong(LAST_TRANSACTION_ID_POS, transactionId);
		writeLong(LAST_TRANSACTION_TIMESTAMP_POS, System.currentTimeMillis());
		return transactionId;
	}

	public long getLastTransactionTimestamp() {
		return readLong(LAST_TRANSACTION_TIMESTAMP_POS);
	}


	private long readLong(int pos) {
		return buffer.getLongVolatile(pos);
	}

	private void writeLong(int pos, long value) {
		buffer.putLongVolatile(pos, value);
	}

	private int readInt(int pos) {
		return buffer.getIntVolatile(pos);
	}

	private void writeInt(int pos, int value) {
		buffer.putIntVolatile(pos, value);
	}

	private String readString(int pos) {
		int length = buffer.getIntVolatile(pos);
		byte[] bytes = new byte[length];
		buffer.getBytes(pos + 4, bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	private void writeString(int pos, String s) {
		byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
		buffer.putIntVolatile(pos, bytes.length);
		buffer.putBytes(pos + 4, bytes);
	}


	@Override
	public long getLastCommittedTransactionId() {
		return getLastTransactionId();
	}

	@Override
	public long getNextTransactionId() {
		return setNextTransactionId();
	}
}
