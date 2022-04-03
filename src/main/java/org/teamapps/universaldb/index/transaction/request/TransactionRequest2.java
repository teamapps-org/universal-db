package org.teamapps.universaldb.index.transaction.request;

import java.io.*;
import java.util.*;

public class TransactionRequest2 {

	private final long nodeId;
	private final long transactionRequestId;
	private final int userId;
	private final long timestamp = System.currentTimeMillis();
	private final List<TransactionRequestRecord> records = new ArrayList<>();

	private final Set<Integer> createRecordCorrelationSet = new HashSet<>();
	private final Map<Integer, Integer> recordIdByCorrelationId = new HashMap<>();


	public TransactionRequest2(long nodeId, long transactionRequestId, int userId) {
		this.nodeId = nodeId;
		this.transactionRequestId = transactionRequestId;
		this.userId = userId;
	}

	public TransactionRequest2(byte[] bytes) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
		this.nodeId = dis.readLong();
		this.transactionRequestId = dis.readLong();
		this.userId = dis.readInt();
		int size = dis.readInt();
		for (int i = 0; i < size; i++) {
			records.add(new TransactionRequestRecord(dis));
		}
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeLong(nodeId);
		dos.writeLong(transactionRequestId);
		dos.writeInt(userId);
		dos.writeLong(timestamp);
		dos.writeInt(records.size());
		for (TransactionRequestRecord record : records) {
			record.write(dos);
		}
		return bos.toByteArray();
	}

	public void addRecord(TransactionRequestRecord record) {
		if (record.getRecordType() != TransactionRequestRecordType.CREATE || !createRecordCorrelationSet.contains(record.getCorrelationId())) {
			createRecordCorrelationSet.add(record.getCorrelationId());
			records.add(record);
		}
	}

	public long getNodeId() {
		return nodeId;
	}

	public long getTransactionRequestId() {
		return transactionRequestId;
	}

	public int getUserId() {
		return userId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public List<TransactionRequestRecord> getRecords() {
		return records;
	}

	public int getResolvedRecordIdByCorrelationId(int correlationId) {
		if (recordIdByCorrelationId.containsKey(correlationId)) {
			return recordIdByCorrelationId.get(correlationId);
		} else {
			return 0;
		}
	}

	public void putResolvedRecordIdForCorrelationId(int correlationId, int recordId) {
		recordIdByCorrelationId.put(correlationId, recordId);
	}

	public Map<Integer, Integer> getRecordIdByCorrelationId() {
		return recordIdByCorrelationId;
	}
}
