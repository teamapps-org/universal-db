/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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

import org.teamapps.universaldb.index.FieldIndex;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.pojo.EntityChangeSet;
import org.teamapps.universaldb.schema.Table;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TransactionRequestRecord {

	private final TransactionRequestRecordType recordType;
	private final int tableId;
	private final int recordId;
	private final int correlationId;
	private final List<TransactionRequestRecordValue> recordValues = new ArrayList<>();
	private boolean transactionProcessingStarted;

	public static TransactionRequestRecord createOrUpdateRecord(TransactionRequest transactionRequest, TableIndex tableIndex, int recordId, int correlationId, boolean update, EntityChangeSet entityChangeSet) {
		TransactionRequestRecordType type = update ? TransactionRequestRecordType.UPDATE : TransactionRequestRecordType.CREATE;
		if (!update && recordId > 0) {
			type = TransactionRequestRecordType.CREATE_WITH_ID;
		}
		TransactionRequestRecord requestRecord = new TransactionRequestRecord(type, tableIndex.getMappingId(), recordId, correlationId);
		requestRecord.createMetaData(tableIndex, transactionRequest.getUserId(), transactionRequest.getTimestamp(), entityChangeSet);
		return requestRecord;
	}

	public static TransactionRequestRecord createDeleteRecord(TransactionRequest transactionRequest, TableIndex tableIndex, int recordId, EntityChangeSet entityChangeSet) {
		TransactionRequestRecord requestRecord = new TransactionRequestRecord(TransactionRequestRecordType.DELETE, tableIndex.getMappingId(), recordId, 0);
		requestRecord.createMetaData(tableIndex, transactionRequest.getUserId(), transactionRequest.getTimestamp(), entityChangeSet);
		if (recordId <= 0) {
			throw new RuntimeException("Cannot delete record with id:" + recordId);
		}
		return requestRecord;
	}

	public static TransactionRequestRecord createRestoreRecord(TransactionRequest transactionRequest, TableIndex tableIndex, int recordId, EntityChangeSet entityChangeSet) {
		TransactionRequestRecord requestRecord = new TransactionRequestRecord(TransactionRequestRecordType.RESTORE, tableIndex.getMappingId(), recordId, 0);
		requestRecord.createMetaData(tableIndex, transactionRequest.getUserId(), transactionRequest.getTimestamp(), entityChangeSet);
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

	public void createMetaData(TableIndex tableIndex, int userId, long timestamp, EntityChangeSet entityChangeSet) {
		int changeDate = (int) (timestamp / 1000);
		switch (recordType) {
			case CREATE:
			case CREATE_WITH_ID:
				if (tableIndex.getTableModel().isTrackModifications()) {
					FieldIndex<?, ?> createDateIndex = tableIndex.getFieldIndex(Table.FIELD_CREATION_DATE);
					if (!entityChangeSet.isChanged(createDateIndex)) {
						addRecordValue(createDateIndex, changeDate);
					}
					FieldIndex<?, ?> createByIndex = tableIndex.getFieldIndex(Table.FIELD_CREATED_BY);
					if (!entityChangeSet.isChanged(createByIndex)) {
						addRecordValue(createByIndex, userId);
					}
					FieldIndex<?, ?> modificationDateIndex = tableIndex.getFieldIndex(Table.FIELD_MODIFICATION_DATE);
					if (!entityChangeSet.isChanged(modificationDateIndex)) {
						addRecordValue(modificationDateIndex, changeDate);
					}
					FieldIndex<?, ?> modifiedByIndex = tableIndex.getFieldIndex(Table.FIELD_MODIFIED_BY);
					if (!entityChangeSet.isChanged(modifiedByIndex)) {
						addRecordValue(modifiedByIndex, userId);
					}
				}
				break;
			case UPDATE:
				if (tableIndex.getTableModel().isTrackModifications()) {
					FieldIndex<?, ?> modificationDateIndex = tableIndex.getFieldIndex(Table.FIELD_MODIFICATION_DATE);
					if (!entityChangeSet.isChanged(modificationDateIndex)) {
						addRecordValue(modificationDateIndex, changeDate);
					}
					FieldIndex<?, ?> modifiedByIndex = tableIndex.getFieldIndex(Table.FIELD_MODIFIED_BY);
					if (!entityChangeSet.isChanged(modifiedByIndex)) {
						addRecordValue(modifiedByIndex, userId);
					}
				}
				break;
			case DELETE:
				if (tableIndex.getTableModel().isRecoverableRecords()) {
					FieldIndex<?, ?> deletionDateIndex = tableIndex.getFieldIndex(Table.FIELD_DELETION_DATE);
					if (entityChangeSet == null || !entityChangeSet.isChanged(deletionDateIndex)) {
						addRecordValue(deletionDateIndex, changeDate);
					}
					FieldIndex<?, ?> deletedByIndex = tableIndex.getFieldIndex(Table.FIELD_DELETED_BY);
					if (entityChangeSet == null || !entityChangeSet.isChanged(deletedByIndex)) {
						addRecordValue(deletedByIndex, userId);
					}
				}
				break;
			case RESTORE:
				if (tableIndex.getTableModel().isRecoverableRecords()) {
					FieldIndex<?, ?> restoreDateIndex = tableIndex.getFieldIndex(Table.FIELD_RESTORE_DATE);
					if (entityChangeSet == null || !entityChangeSet.isChanged(restoreDateIndex)) {
						addRecordValue(restoreDateIndex, changeDate);
					}
					FieldIndex<?, ?> restoredByIndex = tableIndex.getFieldIndex(Table.FIELD_RESTORED_BY);
					if (entityChangeSet == null || !entityChangeSet.isChanged(restoredByIndex)) {
						addRecordValue(restoredByIndex, userId);
					}
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

	public void addRecordValue(FieldIndex fieldIndex, Object value) {
		addRecordValue(new TransactionRequestRecordValue(fieldIndex.getMappingId(), fieldIndex.getType(), value));
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
