package org.teamapps.universaldb.index.transaction.resolved;

import org.teamapps.universaldb.index.transaction.request.TransactionRequest2;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class ResolvedTransaction {

	private final long transactionId;
	private final int userId;
	private final long timestamp;
	private final List<ResolvedTransactionRecord> transactionRecords = new ArrayList<>();

	public static ResolvedTransaction createResolvedTransaction(byte[] bytes) {
		try {
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
			return new ResolvedTransaction(dis);
		} catch (IOException e) {
			throw new RuntimeException("Cannot parse transaction", e);
		}
	}

	public static ResolvedTransaction createFromRequest(long transactionId, TransactionRequest2 transactionRequest) {
		return new ResolvedTransaction(transactionId, transactionRequest.getUserId(), transactionRequest.getTimestamp());
	}


	public ResolvedTransaction(long transactionId, int userId, long timestamp) {
		this.transactionId = transactionId;
		this.userId = userId;
		this.timestamp = timestamp;
	}

	public ResolvedTransaction(DataInputStream dis) throws IOException {
		transactionId = dis.readLong();
		userId = dis.readInt();
		timestamp = dis.readLong();
		int count = dis.readInt();
		for (int i = 0; i < count; i++) {
			transactionRecords.add(new ResolvedTransactionRecord(dis));
		}
	}

	public void write(DataOutputStream dos) throws IOException {
		dos.writeLong(transactionId);
		dos.writeInt(userId);
		dos.writeLong(timestamp);
		dos.writeInt(transactionRecords.size());
		for (ResolvedTransactionRecord transactionRecord : transactionRecords) {
			transactionRecord.write(dos);
		}
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		write(dos);
		return bos.toByteArray();
	}

	public void addTransactionRecord(ResolvedTransactionRecord transactionRecord) {
		transactionRecords.add(transactionRecord);
	}

	public long getTransactionId() {
		return transactionId;
	}

	public int getUserId() {
		return userId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public List<ResolvedTransactionRecord> getTransactionRecords() {
		return transactionRecords;
	}
}
