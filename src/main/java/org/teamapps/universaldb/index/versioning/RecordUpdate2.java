package org.teamapps.universaldb.index.versioning;

import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransaction;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecord;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecordType;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecordValue;

import java.io.*;
import java.util.List;

public class RecordUpdate2 {

	private long previousPosition;
	private final ResolvedTransactionRecord transactionRecord;
	private final int userId;
	private final long timestamp;
	private final long transactionId;

	public static RecordUpdate2 createUpdate(ResolvedTransaction transaction, ResolvedTransactionRecord record) {
		return new RecordUpdate2(record, transaction.getUserId(), transaction.getTimestamp(), transaction.getTransactionId());
	}

	public RecordUpdate2(ResolvedTransactionRecord transactionRecord, int userId, long timestamp, long transactionId) {
		this.transactionRecord = transactionRecord;
		this.userId = userId;
		this.timestamp = timestamp;
		this.transactionId = transactionId;
	}

	public RecordUpdate2(byte[] bytes) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
		previousPosition = dis.readLong();
		transactionRecord = new ResolvedTransactionRecord(dis);
		userId = dis.readInt();
		timestamp = dis.readLong();
		transactionId = dis.readLong();
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeLong(previousPosition);
		transactionRecord.write(dos);
		dos.writeInt(userId);
		dos.writeLong(timestamp);
		dos.writeLong(transactionId);
		return bos.toByteArray();
	}

	public long getPreviousPosition() {
		return previousPosition;
	}

	public void setPreviousPosition(long previousPosition) {
		this.previousPosition = previousPosition;
	}

	public ResolvedTransactionRecord getTransactionRecord() {
		return transactionRecord;
	}

	public int getUserId() {
		return userId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public ResolvedTransactionRecordType getRecordType() {
		return transactionRecord.getRecordType();
	}

	public List<ResolvedTransactionRecordValue> getRecordValues() {
		return transactionRecord.getRecordValues();
	}

	public int getTableId() {
		return transactionRecord.getTableId();
	}

	public int getRecordId() {
		return transactionRecord.getRecordId();
	}
}
