/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.index.DataBaseMapper;
import org.teamapps.universaldb.index.TableIndex;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ClusterTransaction {

	private static final Logger log = LoggerFactory.getLogger(ClusterTransaction.class);
	private static final AtomicLong transactionRequestIdGenerator = new AtomicLong();

	private final long transactionRequestId; //2B node id, 2B counter, 4B timestamp
	private final long timestamp;
	private final int userId;
	private long transactionId;

	private List<TransactionRecord> transactionRecords;
	private Map<Integer, Integer> recordIdByCorrelationId;
	private Map<Integer, TransactionRecord> transactionRecordByCorrelationId;

	public ClusterTransaction(int userId) {
		this(transactionRequestIdGenerator.incrementAndGet(), userId);
	}

	private ClusterTransaction(long transactionRequestId, int userId) {
		this.transactionRequestId = transactionRequestId;
		this.timestamp = System.currentTimeMillis();
		this.userId = userId;
		transactionRecords = new ArrayList<>();
		recordIdByCorrelationId = new HashMap<>();
		transactionRecordByCorrelationId = new HashMap<>();
	}

	public ClusterTransaction(TransactionPacket packet, DataBaseMapper dataBaseMapper) throws IOException {
		this.transactionRequestId = packet.getTransactionRequestId();
		this.transactionId = packet.getTransactionId();
		this.timestamp = packet.getTimestamp();
		this.userId = packet.getUserId();
		this.transactionRecordByCorrelationId = new HashMap<>();
		this.transactionRecords = new ArrayList<>();
		DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(packet.getTransactionData()));
		int records = dataInputStream.readInt();
		for (int i = 0; i < records; i++) {
			TransactionRecord transactionRecord = new TransactionRecord(dataInputStream, dataBaseMapper);
			addTransactionRecord(transactionRecord);
		}

		recordIdByCorrelationId = new HashMap<>();
		if (packet.getCorrelationData() != null) {
			dataInputStream = new DataInputStream(new ByteArrayInputStream(packet.getCorrelationData()));
			int mapSize = dataInputStream.readInt();
			for (int i = 0; i < mapSize; i++) {
				int key = dataInputStream.readInt();
				int value = dataInputStream.readInt();
				recordIdByCorrelationId.put(key, value);
			}
		}
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getUserId() {
		return userId;
	}

	public TransactionRequest createRequest() {
		return new TransactionRequest(this);
	}

	public void addTransactionRecord(TransactionRecord transactionRecord) {
		if (transactionRecord.getRecordId() == 0) {
			if (transactionRecordByCorrelationId.containsKey(transactionRecord.getCorrelationId())) {
				log.info("Omitted repeated transaction record processing steps - this log just indicates that the processed entity referenced an uncommitted entity");
				return;
			} else {
				transactionRecordByCorrelationId.put(transactionRecord.getCorrelationId(), transactionRecord);
			}
		}
		transactionRecords.add(transactionRecord);
	}

	public long getTransactionRequestId() {
		return transactionRequestId;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(long transactionId) {
		this.transactionId = transactionId;
	}

	public List<TransactionRecord> getTransactionRecords() {
		return transactionRecords;
	}

	public void setTransactionRecords(List<TransactionRecord> transactionRecords) {
		this.transactionRecords = transactionRecords;
	}

	public void writeTransactionData(DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(transactionRecords.size());
		for (TransactionRecord transactionRecord : transactionRecords) {
			transactionRecord.writeTransactionValue(dataOutputStream);
		}
	}

	public void writeTransactionCorrelationData(DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(recordIdByCorrelationId.size());
		for (Map.Entry<Integer, Integer> entry : recordIdByCorrelationId.entrySet()) {
			dataOutputStream.writeInt(entry.getKey());
			dataOutputStream.writeInt(entry.getValue());
		}
	}

	public TransactionPacket createTransactionPacket() throws IOException {
		TransactionPacket transactionPacket = new TransactionPacket(transactionRequestId, transactionId, timestamp, userId);

		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		writeTransactionData(new DataOutputStream(byteArrayOutputStream));
		transactionPacket.setTransactionData(byteArrayOutputStream.toByteArray());

		if (recordIdByCorrelationId.size() > 0) {
			byte[] correlationData = getCorrelationData();
			transactionPacket.setCorrelationData(correlationData);
		}
		return transactionPacket;
	}

	private byte[] getCorrelationData() throws IOException {
		ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream();
		writeTransactionCorrelationData(new DataOutputStream(byteArrayStream));
		return byteArrayStream.toByteArray();
	}

	public TransactionPacket resolveAndExecuteTransaction(TransactionIdProvider transactionIdProvider, TransactionPacket packet) throws IOException {
		transactionId = transactionIdProvider.getNextTransactionId();

		boolean dataChangeCheckSuccess = true;
		for (TransactionRecord transactionRecord : transactionRecords) {
			if (!transactionRecord.checkUnchangedRecordTransactionId()) {
				dataChangeCheckSuccess = false;
				break;
			}
		}

		if (!dataChangeCheckSuccess) {
			log.warn("Transaction failed with strict data change check!");
			//todo: send failed transaction message
			return null;
		}

		for (TransactionRecord transactionRecord : transactionRecords) {
			transactionRecord.createIfNotExists(recordIdByCorrelationId);
		}

		for (TransactionRecord transactionRecord : transactionRecords) {
			transactionRecord.persistChanges(transactionId, recordIdByCorrelationId);
		}

		if (packet == null) {
			return createTransactionPacket();
		} else {
			packet.setTransactionId(transactionId);
			if (recordIdByCorrelationId.size() > 0) {
				packet.setCorrelationData(getCorrelationData());
			}
			return packet;
		}
	}

	public void executeResolvedTransaction() {
		for (TransactionRecord transactionRecord : transactionRecords) {
			transactionRecord.persistResolvedChanges(transactionId, recordIdByCorrelationId);
		}
	}

	public int getResolvedRecordIdByCorrelationId(int correlationId) {
		if (recordIdByCorrelationId.containsKey(correlationId)) {
			return recordIdByCorrelationId.get(correlationId);
		} else {
			return 0;
		}
	}


}
