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
import org.teamapps.universaldb.TableConfig;
import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.DataBaseMapper;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.text.FullTextIndexValue;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.schema.Table;

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
	private final boolean restoreRecord;
	private final int correlationId;
	private final long recordTransactionId;
	private List<TransactionRecordValue> recordValues;

	private boolean transactionProcessingStarted = false;


	public static TransactionRecord createOrUpdateRecord(TableIndex tableIndex, int recordId, int correlationId, int userId, boolean update, boolean strictChangeVerification) {
		return new TransactionRecord(tableIndex, recordId, correlationId, userId, update, false, strictChangeVerification, false);
	}

	public static TransactionRecord deleteRecord(TableIndex tableIndex, int recordId, int userId) {
		return new TransactionRecord(tableIndex, recordId, 0, userId, false, true, false, false);
	}

	public static TransactionRecord restoreRecord(TableIndex tableIndex, int recordId, int userId) {
		return new TransactionRecord(tableIndex, recordId, 0, userId, false, false, false, true);
	}

	private TransactionRecord(TableIndex tableIndex, int recordId, int correlationId, int userId, boolean update, boolean deleteRecord, boolean strictChangeVerification, boolean restoreRecord) {
		this.tableIndex = tableIndex;
		this.recordId = recordId;
		this.correlationId = correlationId;
		this.update = update;
		this.deleteRecord = deleteRecord;
		this.restoreRecord = restoreRecord;
		this.recordTransactionId = strictChangeVerification ? tableIndex.getTransactionId(recordId) : 0;
		recordValues = new ArrayList<>();
		if (deleteRecord) {
			if (recordId <= 0) {
				throw new RuntimeException("Cannot delete record with no record id");
			}
			setDeletionData(userId);
		} else if (restoreRecord) {
			if (recordId <= 0) {
				throw new RuntimeException("Cannot restore record with no record id");
			}
			setRestoreData(userId);
		} else {
			setModificationData(tableIndex, update, userId);
		}
	}

	private void setModificationData(TableIndex tableIndex, boolean update, int userId) {
		TableConfig config = tableIndex.getTableConfig();
		int changeDate = (int) (System.currentTimeMillis() / 1000);
		if (!update) {
			if (config.trackCreation()) {
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_CREATION_DATE), changeDate);
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_CREATED_BY), userId);
			}
			if (config.trackModification()) {
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFICATION_DATE), changeDate);
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFIED_BY), userId);
			}
		} else {
			if (config.trackModification()) {
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFICATION_DATE), changeDate);
				addRecordValue(tableIndex.getColumnIndex(Table.FIELD_MODIFIED_BY), userId);
			}
		}
	}

	private void setDeletionData(int userId) {
		TableConfig config = tableIndex.getTableConfig();
		if (config.keepDeleted()) {
			addRecordValue(tableIndex.getColumnIndex(Table.FIELD_DELETION_DATE), (int) (System.currentTimeMillis() / 1000));
			addRecordValue(tableIndex.getColumnIndex(Table.FIELD_DELETED_BY), userId);
		}
	}

	private void setRestoreData(int userId) {
		TableConfig config = tableIndex.getTableConfig();
		if (config.keepDeleted()) {
			addRecordValue(tableIndex.getColumnIndex(Table.FIELD_RESTORE_DATE), (int) (System.currentTimeMillis() / 1000));
			addRecordValue(tableIndex.getColumnIndex(Table.FIELD_RESTORED_BY), userId);
		}
	}

	public TransactionRecord(DataInputStream dis, DataBaseMapper dataBaseMapper) throws IOException {
		this.tableIndex = dataBaseMapper.getCollectionIndexById(dis.readInt());
		this.recordId = dis.readInt();
		this.correlationId = dis.readInt();
		this.update = dis.readBoolean();
		this.deleteRecord = dis.readBoolean();
		this.restoreRecord = dis.readBoolean();
		this.recordTransactionId = dis.readLong();
		recordValues = new ArrayList<>();
		int valueCount = dis.readInt();
		for (int i = 0; i < valueCount; i++) {
			recordValues.add(new TransactionRecordValue(dis, dataBaseMapper));
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

	public boolean isUpdate() {
		return update;
	}

	public boolean isDeleteRecord() {
		return deleteRecord;
	}

	public long getRecordTransactionId() {
		return recordTransactionId;
	}

	public void writeTransactionValue(DataOutputStream dos) throws IOException {
		dos.writeInt(tableIndex.getMappingId());
		dos.writeInt(recordId);
		dos.writeInt(correlationId);
		dos.writeBoolean(update);
		dos.writeBoolean(deleteRecord);
		dos.writeBoolean(restoreRecord);
		dos.writeLong(recordTransactionId);
		dos.writeInt(recordValues.size());
		for (TransactionRecordValue recordValue : recordValues) {
			recordValue.writeTransactionValue(dos);
		}
	}

	public boolean checkUnchangedRecordTransactionId() {
		if (recordTransactionId == 0 || recordId == 0) {
			return true;
		} else {
			return recordTransactionId == tableIndex.getTransactionId(recordId);
		}
	}

	public void createIfNotExists(Map<Integer, Integer> recordIdByCorrelationId) {
		if (!deleteRecord && !restoreRecord) {
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
			tableIndex.deleteRecord(recordId);
			processChanges = false;
		} else if (restoreRecord) {
			tableIndex.restoreRecord(recordId);
			processChanges = false;
		} else if (recordId == 0) {
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
			tableIndex.deleteRecord(recordId);
			processChanges = false;
		} else if (restoreRecord) {
			tableIndex.restoreRecord(recordId);
			processChanges = false;
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
		if (!deleteRecord && !restoreRecord) {
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
