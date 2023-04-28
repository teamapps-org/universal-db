/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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
package org.teamapps.universaldb.index.transaction.request;

import org.teamapps.universaldb.index.transaction.TransactionType;
import org.teamapps.universaldb.model.DatabaseModel;

import java.io.*;
import java.util.*;

public class TransactionRequest {

	private final long nodeId;
	private final long requestId;
	private final TransactionType transactionType;
	private final int userId;
	private final long timestamp;
	private final List<TransactionRequestRecord> records = new ArrayList<>();

	private DatabaseModel databaseModel;
	private final Set<Integer> createRecordCorrelationSet = new HashSet<>();
	private final Map<Integer, Integer> recordIdByCorrelationId = new HashMap<>();


	public TransactionRequest(long nodeId, long requestId, int userId) {
		this.nodeId = nodeId;
		this.requestId = requestId;
		this.transactionType = TransactionType.DATA_UPDATE;
		this.userId = userId;
		this.timestamp = System.currentTimeMillis();
	}

	public TransactionRequest(long nodeId, long requestId, int userId, DatabaseModel databaseModel) {
		this.nodeId = nodeId;
		this.requestId = requestId;
		this.transactionType = TransactionType.MODEL_UPDATE;
		this.userId = userId;
		this.timestamp = System.currentTimeMillis();
		this.databaseModel = databaseModel;
	}

	public TransactionRequest(byte[] bytes) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
		this.nodeId = dis.readLong();
		this.requestId = dis.readLong();
		this.transactionType = TransactionType.getById(dis.readUnsignedByte());
		this.userId = dis.readInt();
		this.timestamp = dis.readLong();
		if (transactionType == TransactionType.DATA_UPDATE) {
			int size = dis.readInt();
			for (int i = 0; i < size; i++) {
				records.add(new TransactionRequestRecord(dis));
			}
		} else {
			this.databaseModel = new DatabaseModel(dis);
		}
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeLong(nodeId);
		dos.writeLong(requestId);
		dos.writeByte(transactionType.getId());
		dos.writeInt(userId);
		dos.writeLong(timestamp);
		if (transactionType == TransactionType.DATA_UPDATE) {
			dos.writeInt(records.size());
			for (TransactionRequestRecord record : records) {
				record.write(dos);
			}
		} else {
			databaseModel.write(dos);
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

	public long getRequestId() {
		return requestId;
	}

	public TransactionType getTransactionType() {
		return transactionType;
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

	public DatabaseModel getDatabaseModel() {
		return databaseModel;
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
