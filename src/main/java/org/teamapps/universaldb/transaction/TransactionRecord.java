/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
import org.teamapps.universaldb.TableConfig;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.schema.Table;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.text.FullTextIndexValue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransactionRecord {

	private static final Logger log = LoggerFactory.getLogger(TransactionRecord.class);

	private final TableIndex tableIndex;
	private int recordId;
	private final boolean update;
	private final boolean deleteRecord;
	private final int correlationId;
	private final long recordTransactionId;
	private List<TransactionRecordValue> recordValues;

	private boolean transactionProcessingStarted = false;

	public TransactionRecord(TableIndex tableIndex, int recordId, int correlationId, int userId) {
		this(tableIndex, recordId, correlationId, userId, recordId > 0, false, false);
	}

	public TransactionRecord(TableIndex tableIndex, int recordId, int correlationId, int userId, boolean deleteRecord) {
		this(tableIndex, recordId, correlationId, userId, deleteRecord, recordId > 0, false);
	}

	public TransactionRecord(TableIndex tableIndex, int recordId, int correlationId, int userId, boolean update, boolean deleteRecord, boolean strictChangeVerification) {
		this.tableIndex = tableIndex;
		this.recordId = recordId;
		this.correlationId = correlationId;
		this.update = update;
		this.deleteRecord = deleteRecord;
		this.recordTransactionId = strictChangeVerification ? tableIndex.getTransactionId(recordId) : 0;
		TableConfig config = tableIndex.getTableConfig();
		recordValues = new ArrayList<>();
		if (deleteRecord) {
			if (recordId <= 0) {
				throw new RuntimeException("Cannot delete record with no record id");
			}
			setDeletionData(tableIndex, userId);
		} else {
			setModificationData(tableIndex, update, userId);
		}
	}

	public void setModificationData(TableIndex tableIndex, boolean update, int userId) {
		TableConfig config = tableIndex.getTableConfig();
		if (!update) {
			if (config.trackCreation()) {
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_CREATION_DATE), (int) (System.currentTimeMillis() / 1000));
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_CREATED_BY), userId);
			}
			if (config.trackModification()) {
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFICATION_DATE), (int) (System.currentTimeMillis() / 1000));
			}
		} else {
			if (config.trackModification()) {
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFICATION_DATE), (int) (System.currentTimeMillis() / 1000));
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFIED_BY), userId);
			}
		}
	}

	public void setDeletionData(TableIndex tableIndex, int userId) {
		TableConfig config = tableIndex.getTableConfig();
		if (config.keepDeleted()) {
			addRecordValue(tableIndex.getColumnIndex(Table.FIELD_DELETION_DATE), (int) (System.currentTimeMillis() / 1000));
			addRecordValue(tableIndex.getColumnIndex(Table.FIELD_DELETED_BY), userId);
		}
	}

	public TransactionRecord(DataInputStream dataInputStream, DataBaseMapper dataBaseMapper) throws IOException {
		this.tableIndex = dataBaseMapper.getCollectionIndexById(dataInputStream.readInt());
		this.recordId = dataInputStream.readInt();
		this.correlationId = dataInputStream.readInt();
		this.update = dataInputStream.readBoolean();
		this.deleteRecord = dataInputStream.readBoolean();
		this.recordTransactionId = dataInputStream.readLong();
		recordValues = new ArrayList<>();
		int valueCount = dataInputStream.readInt();
		for (int i = 0; i < valueCount; i++) {
			recordValues.add(new TransactionRecordValue(dataInputStream, dataBaseMapper));
		}
	}

	public void addRecordValue(ColumnIndex column, Object value) {
		addRecordValue(new TransactionRecordValue(column, value));
	}

	public void addRecordValue(TransactionRecordValue recordValue) {
		recordValues.add(recordValue);
	}

	public int getRecordId() {
		return recordId;
	}

	public int getCorrelationId() {
		return correlationId;
	}

	public List<TransactionRecordValue> getRecordValues() {
		return recordValues;
	}

	public void writeTransactionValue(DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(tableIndex.getMappingId());
		dataOutputStream.writeInt(recordId);
		dataOutputStream.writeInt(correlationId);
		dataOutputStream.writeBoolean(update);
		dataOutputStream.writeBoolean(deleteRecord);
		dataOutputStream.writeLong(recordTransactionId);
		dataOutputStream.writeInt(recordValues.size());
		for (TransactionRecordValue recordValue : recordValues) {
			recordValue.writeTransactionValue(dataOutputStream);
		}
	}

	public boolean checkUnchangedRecordTransactionId(){
		if (recordTransactionId == 0 || recordId == 0) {
			return true;
		} else {
			return recordTransactionId == tableIndex.getTransactionId(recordId);
		}
	}

	public void createIfNotExists(Map<Integer, Integer> recordIdByCorrelationId) {
		if (!deleteRecord) {
			boolean updateMap = recordId == 0;
			recordId = tableIndex.createRecord(recordId, correlationId, update);
			if (updateMap) {
				recordIdByCorrelationId.put(correlationId, recordId);
			}
		}

	}

	public void persistChanges(long transactionId, Map<Integer, Integer> recordIdByCorrelationId) {
		if (transactionProcessingStarted) {
			//make sure that a record that has been processed because of a reference ist not processed again
			return;
		}
		transactionProcessingStarted = true;
		boolean processChanges = true;
		if (deleteRecord) {
			processChanges = tableIndex.deleteRecord(recordId);
		} else if (recordId == 0){
			log.error("ERROR!: could not save record - record id == 0:" + tableIndex.getFQN());
			return;
		}
		if (processChanges) {
			processColumnChanges(transactionId, recordIdByCorrelationId);
		}
	}

	public void persistResolvedChanges(long transactionId, Map<Integer, Integer> recordIdByCorrelationId) {
		boolean processChanges = true;
		if (deleteRecord) {
			processChanges = tableIndex.deleteRecord(recordId);
		} else {
			if (recordId == 0) {
				recordId = recordIdByCorrelationId.get(correlationId);
				if (recordId == 0) {
					log.error("ERROR!: could not resolve recordId by correlationId for table:" + tableIndex.getFQN());
					return;
				}
			}
			recordId = tableIndex.createRecord(recordId, correlationId, update);
		}
		if (processChanges) {
			processColumnChanges(transactionId, recordIdByCorrelationId);
		}
	}

	public void processColumnChanges(long transactionId, Map<Integer, Integer> recordIdByCorrelationId) {
		tableIndex.setTransactionId(recordId, transactionId);
		for (TransactionRecordValue recordValue : recordValues) {
			recordValue.persistChange(recordId, recordIdByCorrelationId);
		}
		if (!deleteRecord) {
			List<FullTextIndexValue> fullTextIndexValues = recordValues.stream()
					.filter(value -> value.getColumn().getType() == IndexType.TEXT || value.getColumn().getType() == IndexType.TRANSLATABLE_TEXT)
					.map(value -> {
						if (value.getColumn().getType() == IndexType.TEXT) {
							return new FullTextIndexValue(value.getColumn().getName(), (String) value.getValue());
						} else {
							return new FullTextIndexValue(value.getColumn().getName(), (TranslatableText) value.getValue());
						}
					})
					.collect(Collectors.toList());
			if (!fullTextIndexValues.isEmpty()) {
				tableIndex.updateFullTextIndex(recordId, fullTextIndexValues, update);
			}
		}
	}
}
