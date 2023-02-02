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

import org.teamapps.universaldb.TableConfig;
import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.schema.Table;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransactionRequestRecord {

	private final TransactionRequestRecordType recordType;
	private final int tableId;
	private final int recordId;
	private final int correlationId;
	private final List<TransactionRequestRecordValue> recordValues = new ArrayList<>();
	private boolean transactionProcessingStarted;

	public static TransactionRequestRecord createOrUpdateRecord(TransactionRequest transactionRequest, TableIndex tableIndex, int recordId, int correlationId, boolean update) {
		TransactionRequestRecordType type = update ? TransactionRequestRecordType.UPDATE : TransactionRequestRecordType.CREATE;
		if (!update && recordId > 0) {
			type = TransactionRequestRecordType.CREATE_WITH_ID;
		}
		TransactionRequestRecord requestRecord = new TransactionRequestRecord(type, tableIndex.getMappingId(), recordId, correlationId);
		requestRecord.createMetaData(tableIndex, transactionRequest.getUserId());
		return requestRecord;
	}

	public static TransactionRequestRecord createDeleteRecord(TransactionRequest transactionRequest, TableIndex tableIndex, int recordId) {
		TransactionRequestRecord requestRecord = new TransactionRequestRecord(TransactionRequestRecordType.DELETE, tableIndex.getMappingId(), recordId, 0);
		requestRecord.createMetaData(tableIndex, transactionRequest.getUserId());
		if (recordId <= 0) {
			throw new RuntimeException("Cannot delete record with id:" + recordId);
		}
		return requestRecord;
	}

	public static TransactionRequestRecord createRestoreRecord(TransactionRequest transactionRequest, TableIndex tableIndex, int recordId) {
		TransactionRequestRecord requestRecord = new TransactionRequestRecord(TransactionRequestRecordType.RESTORE, tableIndex.getMappingId(), recordId, 0);
		requestRecord.createMetaData(tableIndex, transactionRequest.getUserId());
		if (recordId <= 0) {
			throw new RuntimeException("Cannot restore record with id:" + recordId);
		}
		return requestRecord;
	}

	public TransactionRequestRecord(TransactionRequestRecordType recordType, int tableId, int recordId, int correlationId) {
		this.recordType = recordType;
		this.tableId = tableId;
		this.recordId = recordId;
		this.correlationId = correlationId;
	}

	public TransactionRequestRecord(DataInputStream dis) throws IOException {
		recordType = TransactionRequestRecordType.getById(dis.readByte());
		tableId = dis.readInt();
		recordId = dis.readInt();
		correlationId = dis.readInt();
		int count = dis.readInt();
		for (int i = 0; i < count; i++) {
			recordValues.add(new TransactionRequestRecordValue(dis));
		}
	}

	public void createMetaData(TableIndex tableIndex, int userId) {
		TableConfig config = tableIndex.getTableConfig();
		int changeDate = (int) (System.currentTimeMillis() / 1000);
		switch (recordType) {
			case CREATE:
			case CREATE_WITH_ID:
				if (config.trackCreation()) {
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_CREATION_DATE), changeDate);
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_CREATED_BY), userId);
				}
				if (config.trackModification()) {
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFICATION_DATE), changeDate);
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFIED_BY), userId);
				}
				break;
			case UPDATE:
				if (config.trackModification()) {
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFICATION_DATE), changeDate);
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFIED_BY), userId);
				}
				break;
			case DELETE:
				if (config.keepDeleted()) {
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_DELETION_DATE), changeDate);
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_DELETED_BY), userId);
				}
				break;
			case RESTORE:
				if (config.keepDeleted()) {
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_RESTORE_DATE), changeDate);
					addRecordValue(tableIndex.getColumnIndex(Table.FIELD_RESTORED_BY), userId);
				}
				break;
		}
	}

	public void write(DataOutputStream dos) throws IOException {
		dos.writeByte(recordType.getId());
		dos.writeInt(tableId);
		dos.writeInt(recordId);
		dos.writeInt(correlationId);
		dos.writeInt(recordValues.size());
		for (TransactionRequestRecordValue recordValue : recordValues) {
			recordValue.write(dos);
		}
	}

	public int getTableId() {
		return tableId;
	}

	public int getRecordId() {
		return recordId;
	}

	public int getCorrelationId() {
		return correlationId;
	}

	public void addRecordValue(ColumnIndex columnIndex, Object value) {
		addRecordValue(new TransactionRequestRecordValue(columnIndex.getMappingId(), columnIndex.getType(), value));
	}

	public void addRecordValue(TransactionRequestRecordValue recordValue) {
		recordValues.add(recordValue);
	}

	public TransactionRequestRecordType getRecordType() {
		return recordType;
	}

	public List<TransactionRequestRecordValue> getRecordValues() {
		return recordValues;
	}

	public boolean isTransactionProcessingStarted() {
		return transactionProcessingStarted;
	}

	public void setTransactionProcessingStarted(boolean transactionProcessingStarted) {
		this.transactionProcessingStarted = transactionProcessingStarted;
	}
}
