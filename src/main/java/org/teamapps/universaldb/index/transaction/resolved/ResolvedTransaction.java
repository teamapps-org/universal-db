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
package org.teamapps.universaldb.index.transaction.resolved;

import org.teamapps.universaldb.index.transaction.TransactionType;
import org.teamapps.universaldb.index.transaction.request.TransactionRequest;
import org.teamapps.universaldb.index.transaction.schema.SchemaUpdate;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResolvedTransaction {

	private final long nodeId;
	private final long requestId;
	private final long transactionId;
	private final TransactionType transactionType;
	private final int userId;
	private final long timestamp;
	private final List<ResolvedTransactionRecord> transactionRecords;
	private Schema schema;
	private Map<Integer, Integer> recordIdByCorrelationId;

	public static ResolvedTransaction createResolvedTransaction(byte[] bytes) {
		try {
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
			return new ResolvedTransaction(dis);
		} catch (IOException e) {
			throw new RuntimeException("Cannot parse transaction", e);
		}
	}

	public static ResolvedTransaction createFromRequest(long transactionId, TransactionRequest request) {
		if (request.getTransactionType() == TransactionType.DATA_UPDATE) {
			return new ResolvedTransaction(request.getNodeId(), request.getRequestId(), transactionId, request.getUserId(), request.getTimestamp());
		} else {
			return new ResolvedTransaction(request.getNodeId(), request.getRequestId(), transactionId, request.getUserId(), request.getTimestamp(), null);
		}
	}

	public ResolvedTransaction(long nodeId, long requestId, long transactionId, int userId, long timestamp) {
		this.nodeId = nodeId;
		this.requestId = requestId;
		this.transactionId = transactionId;
		this.transactionType = TransactionType.DATA_UPDATE;
		this.userId = userId;
		this.timestamp = timestamp;
		this.transactionRecords = new ArrayList<>();
		this.recordIdByCorrelationId = new HashMap<>();
		this.schema = null;
	}

	public ResolvedTransaction(long nodeId, long requestId,long transactionId, int userId, long timestamp, Schema schema) {
		this.nodeId = nodeId;
		this.requestId = requestId;
		this.transactionId = transactionId;
		this.transactionType = TransactionType.MODEL_UPDATE;
		this.userId = userId;
		this.timestamp = timestamp;
		this.transactionRecords = null;
		this.recordIdByCorrelationId = null;
		this.schema = schema;
	}

	public ResolvedTransaction(DataInputStream dis) throws IOException {
		nodeId = dis.readLong();
		requestId = dis.readLong();
		transactionId = dis.readLong();
		transactionType = TransactionType.getById(dis.readUnsignedByte());
		userId = dis.readInt();
		timestamp = dis.readLong();
		if (transactionType == TransactionType.DATA_UPDATE) {
			transactionRecords = new ArrayList<>();
			recordIdByCorrelationId = new HashMap<>();
			schema = null;
			int count = dis.readInt();
			for (int i = 0; i < count; i++) {
				transactionRecords.add(new ResolvedTransactionRecord(dis));
			}
			int correlationIds = dis.readInt();
			if (correlationIds > 0) {
				for (int i = 0; i < correlationIds; i++) {
					recordIdByCorrelationId.put(dis.readInt(), dis.readInt());
				}
			}
		} else {
			transactionRecords = null;
			recordIdByCorrelationId = null;
			schema = new Schema(dis);
		}
	}

	public void write(DataOutputStream dos, boolean withCorrelationIds) throws IOException {
		dos.writeLong(nodeId);
		dos.writeLong(requestId);
		dos.writeLong(transactionId);
		dos.writeByte(transactionType.getId());
		dos.writeInt(userId);
		dos.writeLong(timestamp);
		if (transactionType == TransactionType.DATA_UPDATE) {
			dos.writeInt(transactionRecords.size());
			for (ResolvedTransactionRecord transactionRecord : transactionRecords) {
				transactionRecord.write(dos);
			}
			if (withCorrelationIds && !recordIdByCorrelationId.isEmpty()) {
				dos.writeInt(recordIdByCorrelationId.size());
				for (Map.Entry<Integer, Integer> entry : recordIdByCorrelationId.entrySet()) {
					dos.writeInt(entry.getKey());
					dos.writeInt(entry.getValue());
				}
			} else {
				dos.writeInt(0);
			}
		} else {
			DataStreamUtil.writeStringWithLengthHeader(dos, schema.getSchemaDefinition());
		}
	}

	public byte[] getBytes() throws IOException {
		return getBytes(false);
	}

	public byte[] getBytes(boolean withCorrelationIds) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		write(dos, withCorrelationIds);
		return bos.toByteArray();
	}

	public void addTransactionRecord(ResolvedTransactionRecord transactionRecord) {
		transactionRecords.add(transactionRecord);
	}

	public long getNodeId() {
		return nodeId;
	}

	public long getRequestId() {
		return requestId;
	}

	public long getTransactionId() {
		return transactionId;
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

	public List<ResolvedTransactionRecord> getTransactionRecords() {
		return transactionRecords;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}

	public SchemaUpdate getSchemaUpdate() {
		return new SchemaUpdate(schema, transactionId, timestamp);
	}

	public Map<Integer, Integer> getRecordIdByCorrelationId() {
		return recordIdByCorrelationId;
	}

	public void setRecordIdByCorrelationId(Map<Integer, Integer> recordIdByCorrelationId) {
		this.recordIdByCorrelationId = recordIdByCorrelationId;
	}
}
