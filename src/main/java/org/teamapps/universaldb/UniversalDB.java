/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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
package org.teamapps.universaldb;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.file.FileIndex;
import org.teamapps.universaldb.index.file.FileStore;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.file.LocalFileStore;
import org.teamapps.universaldb.index.log.LogIterator;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.reference.value.MultiReferenceEditValue;
import org.teamapps.universaldb.index.reference.value.RecordReference;
import org.teamapps.universaldb.index.reference.value.ResolvedMultiReferenceUpdate;
import org.teamapps.universaldb.index.text.FullTextIndexValue;
import org.teamapps.universaldb.index.transaction.TransactionIndex;
import org.teamapps.universaldb.index.transaction.TransactionType;
import org.teamapps.universaldb.index.transaction.request.TransactionRequest;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecord;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordType;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordValue;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransaction;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecord;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecordType;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecordValue;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.pojo.AbstractUdbEntity;
import org.teamapps.universaldb.schema.Database;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.schema.SchemaInfoProvider;
import org.teamapps.universaldb.schema.Table;
import org.teamapps.universaldb.update.RecordUpdateEvent;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;

public class UniversalDB implements DataBaseMapper {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private static final ThreadLocal<Integer> THREAD_LOCAL_USER_ID = ThreadLocal.withInitial(() -> 0);

	private final File storagePath;
	private final SchemaIndex schemaIndex;
	private TransactionIndex transactionIndex;

	private final Map<Integer, DatabaseIndex> databaseById = new HashMap<>();
	private final Map<Integer, TableIndex> tableById = new HashMap<>();
	private final Map<Integer, ColumnIndex> columnById = new HashMap<>();
	private final Map<TableIndex, Class> entityClassByTableIndex = new HashMap<>();
	private final Map<TableIndex, Class> queryClassByTableIndex = new HashMap<>();
	private final Map<String, TableIndex> tableIndexByPath = new HashMap<>();

	private final ArrayBlockingQueue<RecordUpdateEvent> updateEventQueue = new ArrayBlockingQueue<>(25_000);

	public static int getUserId() {
		return THREAD_LOCAL_USER_ID.get();
	}

	public static void setUserId(int userId) {
		THREAD_LOCAL_USER_ID.set(userId);
	}

	public static UniversalDB createStandalone(File storagePath, SchemaInfoProvider schemaInfoProvider) throws Exception {
		LocalFileStore fileStore = new LocalFileStore(new File(storagePath, "file-store"));
		return new UniversalDB(storagePath, schemaInfoProvider, fileStore);
	}

	public static UniversalDB createStandalone(File storagePath, SchemaInfoProvider schemaInfoProvider, FileStore fileStore) throws Exception {
		return new UniversalDB(storagePath, schemaInfoProvider, fileStore);
	}

	private UniversalDB(File storagePath, SchemaInfoProvider schemaInfo, FileStore fileStore) throws Exception {
		AbstractUdbEntity.setDatabase(this);
		this.storagePath = storagePath;
		this.transactionIndex = new TransactionIndex(storagePath);

		Schema schema = schemaInfo.getSchema();
		this.schemaIndex = new SchemaIndex(schema, storagePath);
		this.schemaIndex.setFileStore(fileStore);
		mapSchema(schema);

		installTableClasses(transactionIndex.getCurrentSchema(), UniversalDB.class.getClassLoader(), false);
	}

	public UniversalDB(File storagePath, LogIterator logIterator) throws Exception {
		AbstractUdbEntity.setDatabase(this);
		this.storagePath = storagePath;
		this.transactionIndex = new TransactionIndex(storagePath);
		LocalFileStore fileStore = new LocalFileStore(new File(storagePath, "file-store"));

		Schema schema = new Schema();
		this.schemaIndex = new SchemaIndex(schema, storagePath);
		this.schemaIndex.setFileStore(fileStore);

		long time = System.currentTimeMillis();
		long count = 0;
		while (logIterator.hasNext()) {
			byte[] bytes = logIterator.next();
			ResolvedTransaction transaction = ResolvedTransaction.createResolvedTransaction(bytes);
			handleTransaction(transaction);
			count++;
		}
		logger.info("Imported " + count + " transactions in: " + (System.currentTimeMillis() - time));
	}

	private void mapSchema(Schema schema) {
		if (!transactionIndex.isValidSchema(schema)) {
			throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + transactionIndex.getCurrentSchema() + "\nNew schema is:\n" + schema);
		}
		if (transactionIndex.isSchemaUpdate(schema)) {
			TransactionRequest modelUpdateTransactionRequest = createModelUpdateTransactionRequest(schema);
			executeTransaction(modelUpdateTransactionRequest);
		} else {
			schemaIndex.merge(transactionIndex.getCurrentSchema(), true, this);
			for (DatabaseIndex database : schemaIndex.getDatabases()) {
				databaseById.put(database.getMappingId(), database);
				for (TableIndex table : database.getTables()) {
					tableById.put(table.getMappingId(), table);
					for (ColumnIndex columnIndex : table.getColumnIndices()) {
						columnById.put(columnIndex.getMappingId(), columnIndex);
					}
				}
			}
		}
	}

	private void installTableClasses(Schema schema, ClassLoader classLoader, boolean allSchemaTablesAreMandatory) throws Exception {
		String pojoPath = schema.getPojoNamespace();
		for (Database database : schema.getDatabases()) {
			String path = pojoPath + "." + database.getName().toLowerCase();
			for (Table table : database.getAllTables()) {
				TableIndex tableIndex = table.isView() ? schemaIndex.getTableByPath(table.getReferencedTablePath()) : schemaIndex.getTable(table);
				String tableName = table.getName();
				try {
					String className = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
					Class<?> schemaClass = Class.forName(className, true, classLoader);
					Method method = schemaClass.getDeclaredMethod("setTableIndex", TableIndex.class);
					method.setAccessible(true);
					method.invoke(null, tableIndex);

					if (!table.isView()) {
						entityClassByTableIndex.put(tableIndex, schemaClass);
						tableIndexByPath.put(tableIndex.getFQN(), tableIndex);
						String queryClassName = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1) + "Query";
						Class<?> queryClass = Class.forName(queryClassName, true, classLoader);
						queryClassByTableIndex.put(tableIndex, queryClass);
					}
				} catch (ClassNotFoundException e) {
					if (allSchemaTablesAreMandatory) {
						throw e;
					} else {
						logger.info("Could not load entity class for tableIndex:" + tableIndex.getFQN());
					}
				} catch (Exception e) {
					throw e;
				}
			}
		}
	}

	public void addAuxiliaryModel(SchemaInfoProvider schemaInfo, ClassLoader classLoader) throws Exception {
		Schema schema = schemaInfo.getSchema();
		if (!transactionIndex.isValidSchema(schema)) {
			throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + transactionIndex.getCurrentSchema() + "\nNew schema is:\n" + schema);
		}

		if (transactionIndex.isSchemaUpdate(schema)) {
			TransactionRequest modelUpdateTransactionRequest = createModelUpdateTransactionRequest(schema);
			executeTransaction(modelUpdateTransactionRequest);
		}
		installTableClasses(schema, classLoader, true);
	}

	public Class getEntityClass(TableIndex tableIndex) {
		return entityClassByTableIndex.get(tableIndex);
	}

	public Class getQueryClass(TableIndex tableIndex) {
		return queryClassByTableIndex.get(tableIndex);
	}

	public TableIndex getTableIndexByPath(String path) {
		return tableIndexByPath.get(path);
	}

	public synchronized TransactionRequest createTransactionRequest() {
		return new TransactionRequest(transactionIndex.getNodeId(), transactionIndex.createTransactionRequestId(), getUserId());
	}

	public synchronized TransactionRequest createModelUpdateTransactionRequest(Schema schema) {
		return new TransactionRequest(transactionIndex.getNodeId(), transactionIndex.createTransactionRequestId(), getUserId(), schema);
	}


	public synchronized void createInitialTableTransactions(TableIndex tableIndex) throws Exception {
		if (!tableIndex.getRecordVersioningIndex().isEmpty()) {
			return;
		}
		BitSet records = tableIndex.getRecords();
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			writeInitialTransaction(tableIndex, id, false);
		}
		if (tableIndex.isKeepDeletedRecords()) {
			records = tableIndex.getDeletedRecords();
			for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
				writeInitialTransaction(tableIndex, id, true);
			}
		}
	}

	private void writeInitialTransaction(TableIndex tableIndex, int id, boolean deleted) throws Exception {
		int recordId = id;
		ResolvedTransaction transaction = createInitialTransaction(tableIndex, recordId, false);
		ResolvedTransactionRecord record = new ResolvedTransactionRecord(ResolvedTransactionRecordType.CREATE_WITH_ID, tableIndex.getMappingId(), recordId);
		transaction.addTransactionRecord(record);
		List<ColumnIndex> columnIndices = tableIndex.getColumnIndices().stream().filter(col -> !col.isEmpty(recordId)).collect(Collectors.toList());
		for (ColumnIndex column : columnIndices) {
			ResolvedTransactionRecordValue recordValue = createInitialTransactionRecordValue(column, recordId);
			record.addRecordValue(recordValue);
		}
		tableIndex.getRecordVersioningIndex().writeRecordUpdate(transaction, record);
		transactionIndex.writeTransaction(transaction);
		if (deleted) {
			transaction = createInitialTransaction(tableIndex, recordId, true);
			record = new ResolvedTransactionRecord(ResolvedTransactionRecordType.DELETE, tableIndex.getMappingId(), recordId);
			transaction.addTransactionRecord(record);
			record.addRecordValue(createInitialTransactionRecordValue(tableIndex.getColumnIndex(Table.FIELD_DELETION_DATE), recordId));
			record.addRecordValue(createInitialTransactionRecordValue(tableIndex.getColumnIndex(Table.FIELD_DELETED_BY), recordId));
			tableIndex.getRecordVersioningIndex().writeRecordUpdate(transaction, record);
			transactionIndex.writeTransaction(transaction);
		}
	}

	private ResolvedTransaction createInitialTransaction(TableIndex tableIndex, int recordId, boolean deleted) {
		long transactionId = transactionIndex.getLastTransactionId() + 1;
		int userId = 0;
		int timestamp = 0;
		ColumnIndex dateColumn = tableIndex.getColumnIndex(deleted ? Table.FIELD_DELETION_DATE : Table.FIELD_CREATION_DATE);
		ColumnIndex userRefColumn = tableIndex.getColumnIndex(deleted ? Table.FIELD_DELETED_BY : Table.FIELD_CREATED_BY);
		if (dateColumn != null && userRefColumn != null) {
			userId = (int) userRefColumn.getGenericValue(recordId);
			timestamp = (int) dateColumn.getGenericValue(recordId);
		}
		return new ResolvedTransaction(transactionIndex.getNodeId(), transactionIndex.createTransactionRequestId(), transactionId, userId, timestamp * 1_000L);
	}

	private ResolvedTransactionRecordValue createInitialTransactionRecordValue(ColumnIndex column, int recordId) {
		switch (column.getType()) {
			case BOOLEAN:
			case SHORT:
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case TEXT:
			case TRANSLATABLE_TEXT:
			case BINARY:
				Object value = column.getGenericValue(recordId);
				return new ResolvedTransactionRecordValue(column.getMappingId(), column.getType(), value);
			case REFERENCE:
				SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) column;
				int referencedRecordId = singleReferenceIndex.getValue(recordId);
				return new ResolvedTransactionRecordValue(column.getMappingId(), column.getType(), referencedRecordId);
			case MULTI_REFERENCE:
				MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) column;
				List<Integer> references = multiReferenceIndex.getReferencesAsList(recordId);
				ResolvedMultiReferenceUpdate multiReferenceUpdate = ResolvedMultiReferenceUpdate.createSetReferences(references);
				return new ResolvedTransactionRecordValue(column.getMappingId(), column.getType(), multiReferenceUpdate);
			case FILE:
				FileIndex fileIndex = (FileIndex) column;
				FileValue fileValue = fileIndex.getValue(recordId);
				return new ResolvedTransactionRecordValue(column.getMappingId(), column.getType(), fileValue);
			case FILE_NG:
				break;
		}
		return null;
	}

	public synchronized void executeTransaction(TransactionRequest transaction) {
		try {
			ResolvedTransaction resolvedTransaction = handleTransactionRequest(transaction);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private synchronized ResolvedTransaction handleTransactionRequest(TransactionRequest transactionRequest) throws Exception {
		TransactionType transactionType = transactionRequest.getTransactionType();
		long transactionId = transactionIndex.getLastTransactionId() + 1;
		ResolvedTransaction resolvedTransaction = ResolvedTransaction.createFromRequest(transactionId, transactionRequest);

		if (transactionType == TransactionType.DATA_UPDATE) {
			handleDataUpdateRequest(transactionRequest, resolvedTransaction);
		} else {
			handleModelUpdateRequest(transactionRequest, resolvedTransaction);
		}
		return resolvedTransaction;
	}

	private void handleModelUpdateRequest(TransactionRequest request, ResolvedTransaction resolvedTransaction) throws Exception {
		Schema schema = request.getSchema();
		if (!transactionIndex.isValidSchema(schema)) {
			throw new RuntimeException("Cannot update incompatible schema. Current schema is:\n" + transactionIndex.getCurrentSchema() + "\nNew schema is:\n" + schema);
		}

		Schema localSchema = transactionIndex.getCurrentSchema();
		if (localSchema != null) {
			localSchema.merge(schema);
		} else {
			localSchema = schema;
		}
		localSchema.mapSchema();
		resolvedTransaction.setSchema(localSchema);
		transactionIndex.writeTransaction(resolvedTransaction);

		transactionIndex.writeSchemaUpdate(resolvedTransaction.getSchemaUpdate());
		schemaIndex.merge(localSchema, true, this);

		for (DatabaseIndex database : schemaIndex.getDatabases()) {
			databaseById.put(database.getMappingId(), database);
			for (TableIndex table : database.getTables()) {
				tableById.put(table.getMappingId(), table);
				for (ColumnIndex columnIndex : table.getColumnIndices()) {
					columnById.put(columnIndex.getMappingId(), columnIndex);
				}
			}
		}

	}

	private void handleDataUpdateRequest(TransactionRequest request, ResolvedTransaction resolvedTransaction) throws Exception {
		for (TransactionRequestRecord record : request.getRecords()) {
			if (record.getRecordType() == TransactionRequestRecordType.CREATE || record.getRecordType() == TransactionRequestRecordType.CREATE_WITH_ID) {
				TableIndex tableIndex = getTableIndexById(record.getTableId());
				int recordId = tableIndex.createRecord(record.getRecordId());
				request.putResolvedRecordIdForCorrelationId(record.getCorrelationId(), recordId);
			}
		}

		for (TransactionRequestRecord record : request.getRecords()) {
			TableIndex tableIndex = getTableIndexById(record.getTableId());
			if (record.isTransactionProcessingStarted()) {
				//make sure that a record that has been processed because of a reference ist not processed again
				logger.error("Prevented processing of record again:" + record.getTableId() + ":" + record.getRecordId());
				continue;
			}
			record.setTransactionProcessingStarted(true);

			int recordId = record.getRecordId() != 0 ? record.getRecordId() : request.getResolvedRecordIdByCorrelationId(record.getCorrelationId());
			ResolvedTransactionRecord resolvedRecord = ResolvedTransactionRecord.createFromRequest(record, recordId);
			resolvedTransaction.addTransactionRecord(resolvedRecord);

			switch (record.getRecordType()) {
				case CREATE:
				case CREATE_WITH_ID:
				case UPDATE:
					for (TransactionRequestRecordValue recordValue : record.getRecordValues()) {
						List<CyclicReferenceUpdate> cyclicReferenceUpdates = persistColumnValueUpdates(recordId, recordValue, request.getRecordIdByCorrelationId(), resolvedRecord);
						if (cyclicReferenceUpdates != null && !cyclicReferenceUpdates.isEmpty()) {
							for (CyclicReferenceUpdate referenceUpdate : cyclicReferenceUpdates) {
								resolvedTransaction.addTransactionRecord(ResolvedTransactionRecord.createCyclicRecord(referenceUpdate));
							}
						}
					}
					List<FullTextIndexValue> fullTextIndexValues = record.getRecordValues().stream()
							.filter(value -> value.getIndexType() == IndexType.TEXT || value.getIndexType() == IndexType.TRANSLATABLE_TEXT)
							.map(value -> {
								String columnName = getColumnById(value.getColumnId()).getName();
								return value.getIndexType() == IndexType.TEXT ? new FullTextIndexValue(columnName, (String) value.getValue()) : new FullTextIndexValue(columnName, (TranslatableText) value.getValue());
							})
							.collect(Collectors.toList());
					if (!fullTextIndexValues.isEmpty()) {
						tableIndex.updateFullTextIndex(recordId, fullTextIndexValues, record.getRecordType() == TransactionRequestRecordType.UPDATE);
					}
					break;
				case DELETE:
					tableIndex.deleteRecord(record.getRecordId());
					for (TransactionRequestRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(recordId, recordValue, request.getRecordIdByCorrelationId(), resolvedRecord);
					}
					break;
				case RESTORE:
					tableIndex.restoreRecord(record.getRecordId());
					for (TransactionRequestRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(recordId, recordValue, request.getRecordIdByCorrelationId(), resolvedRecord);
					}
					break;
			}
			addRecordUpdateEvent(resolvedRecord, resolvedTransaction.getUserId());
		}
		transactionIndex.writeTransaction(resolvedTransaction);

		for (ResolvedTransactionRecord transactionRecord : resolvedTransaction.getTransactionRecords()) {
			TableIndex tableIndex = getTableIndexById(transactionRecord.getTableId());
			tableIndex.getRecordVersioningIndex().writeRecordUpdate(resolvedTransaction, transactionRecord);
		}
		resolvedTransaction.setRecordIdByCorrelationId(request.getRecordIdByCorrelationId());
	}

	private synchronized void handleTransaction(ResolvedTransaction transaction) throws Exception {
		if (transaction.getTransactionType() == TransactionType.DATA_UPDATE) {
			handleDataUpdateTransaction(transaction);
		} else {
			handleModelUpdateTransaction(transaction);
		}
	}

	private void handleModelUpdateTransaction(ResolvedTransaction transaction) throws Exception {
		Schema schema = transaction.getSchema();
		Schema localSchema = transactionIndex.getCurrentSchema();
		if (localSchema != null) {
			localSchema.merge(schema);
		} else {
			localSchema = schema;
		}
		localSchema.mapSchema();
		transactionIndex.writeTransaction(transaction);

		transactionIndex.writeSchemaUpdate(transaction.getSchemaUpdate());
		schemaIndex.merge(localSchema, true, this);

		for (DatabaseIndex database : schemaIndex.getDatabases()) {
			databaseById.put(database.getMappingId(), database);
			for (TableIndex table : database.getTables()) {
				tableById.put(table.getMappingId(), table);
				for (ColumnIndex columnIndex : table.getColumnIndices()) {
					columnById.put(columnIndex.getMappingId(), columnIndex);
				}
			}
		}
	}

	private void handleDataUpdateTransaction(ResolvedTransaction transaction) throws Exception {
		for (ResolvedTransactionRecord record : transaction.getTransactionRecords()) {
			TableIndex tableIndex = getTableIndexById(record.getTableId());

			switch (record.getRecordType()) {
				case CREATE:
				case CREATE_WITH_ID:
				case UPDATE:
					if (record.getRecordType() == ResolvedTransactionRecordType.CREATE || record.getRecordType() == ResolvedTransactionRecordType.CREATE_WITH_ID) {
						tableIndex.createRecord(record.getRecordId());
					}
					for (ResolvedTransactionRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(record.getRecordId(), recordValue);
					}
					List<FullTextIndexValue> fullTextIndexValues = record.getRecordValues().stream()
							.filter(value -> value.getIndexType() == IndexType.TEXT || value.getIndexType() == IndexType.TRANSLATABLE_TEXT)
							.map(value -> {
								String columnName = getColumnById(value.getColumnId()).getName();
								return value.getIndexType() == IndexType.TEXT ? new FullTextIndexValue(columnName, (String) value.getValue()) : new FullTextIndexValue(columnName, (TranslatableText) value.getValue());
							})
							.collect(Collectors.toList());
					if (!fullTextIndexValues.isEmpty()) {
						tableIndex.updateFullTextIndex(record.getRecordId(), fullTextIndexValues, record.getRecordType() == ResolvedTransactionRecordType.UPDATE);
					}
					break;
				case DELETE:
					tableIndex.deleteRecord(record.getRecordId());
					for (ResolvedTransactionRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(record.getRecordId(), recordValue);
					}
					break;
				case RESTORE:
					tableIndex.restoreRecord(record.getRecordId());
					for (ResolvedTransactionRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(record.getRecordId(), recordValue);
					}
					break;
				case ADD_CYCLIC_REFERENCE:
					break;
				case REMOVE_CYCLIC_REFERENCE:
					break;
			}
			addRecordUpdateEvent(record, transaction.getUserId());
		}
		transactionIndex.writeTransaction(transaction);

		for (ResolvedTransactionRecord transactionRecord : transaction.getTransactionRecords()) {
			TableIndex tableIndex = getTableIndexById(transactionRecord.getTableId());
			tableIndex.getRecordVersioningIndex().writeRecordUpdate(transaction, transactionRecord);
		}
	}

	private List<CyclicReferenceUpdate> persistColumnValueUpdates(int recordId, TransactionRequestRecordValue recordValue, Map<Integer, Integer> recordIdByCorrelationId, ResolvedTransactionRecord resolvedRecord) {
		ColumnIndex column = getColumnById(recordValue.getColumnId());
		Object value = recordValue.getValue();
		if (column.getType() == IndexType.MULTI_REFERENCE) {
			MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) column;
			MultiReferenceEditValue editValue = (MultiReferenceEditValue) value;
			editValue.updateReferences(recordIdByCorrelationId);
			ResolvedMultiReferenceUpdate resolvedUpdateValue = editValue.getResolvedUpdateValue();
			resolvedRecord.addRecordValue(new ResolvedTransactionRecordValue(recordValue.getColumnId(), recordValue.getIndexType(), resolvedUpdateValue));
			return multiReferenceIndex.setReferenceEditValue(recordId, editValue);
		} else if (column.getType() == IndexType.REFERENCE) {
			SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) column;
			if (value != null) {
				RecordReference recordReference = (RecordReference) value;
				recordReference.updateReference(recordIdByCorrelationId);
				resolvedRecord.addRecordValue(new ResolvedTransactionRecordValue(recordValue.getColumnId(), recordValue.getIndexType(), recordReference.getRecordId()));
				return singleReferenceIndex.setReferenceValue(recordId, recordReference);
			} else {
				resolvedRecord.addRecordValue(new ResolvedTransactionRecordValue(recordValue.getColumnId(), recordValue.getIndexType(), null));
				return singleReferenceIndex.setReferenceValue(recordId, null);
			}
		} else {
			column.setGenericValue(recordId, value);
			resolvedRecord.addRecordValue(new ResolvedTransactionRecordValue(recordValue.getColumnId(), recordValue.getIndexType(), value));
		}
		return null;
	}

	private void persistColumnValueUpdates(int recordId, ResolvedTransactionRecordValue recordValue) {
		ColumnIndex column = getColumnById(recordValue.getColumnId());
		Object value = recordValue.getValue();
		if (column.getType() == IndexType.MULTI_REFERENCE) {
			MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) column;
			ResolvedMultiReferenceUpdate multiReferenceUpdate = (ResolvedMultiReferenceUpdate) value;
			multiReferenceIndex.setResolvedReferenceEditValue(recordId, multiReferenceUpdate);
		} else if (column.getType() == IndexType.REFERENCE) {
			SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) column;
			if (value != null) {
				int referencedRecordId = (int) value;
				singleReferenceIndex.setValue(recordId, referencedRecordId, false);
			} else {
				singleReferenceIndex.setValue(recordId, 0, false);
			}
		} else {
			column.setGenericValue(recordId, value);
		}
	}

	public void createDatabaseDump(File dumpFolder) throws IOException {
		for (DatabaseIndex database : schemaIndex.getDatabases()) {
			File dbFolder = new File(dumpFolder, database.getName());
			dbFolder.mkdir();
			for (TableIndex table : database.getTables()) {
				File tableFolder = new File(dbFolder, table.getName());
				tableFolder.mkdir();
				BitSet records = table.getRecords();
				for (ColumnIndex columnIndex : table.getColumnIndices()) {
					File dumpFile = new File(tableFolder, columnIndex.getName() + ".dbd");
					columnIndex.dumpIndex(dumpFile, records); //todo catch, continue and rethrow?
				}
			}
		}
	}


	private void addRecordUpdateEvent(ResolvedTransactionRecord resolvedRecord, int userId) {
		if (userId > 0) {
			RecordUpdateEvent updateEvent = new RecordUpdateEvent(resolvedRecord.getTableId(), resolvedRecord.getRecordId(), userId, resolvedRecord.getRecordType().getUpdateType());
			updateEventQueue.offer(updateEvent);
		}
	}

	@Override
	public DatabaseIndex getDatabaseById(int mappingId) {
		return databaseById.get(mappingId);
	}

	@Override
	public TableIndex getTableIndexById(int mappingId) {
		return tableById.get(mappingId);
	}

	@Override
	public ColumnIndex getColumnById(int mappingId) {
		return columnById.get(mappingId);
	}

	public SchemaIndex getSchemaIndex() {
		return schemaIndex;
	}

	public ArrayBlockingQueue<RecordUpdateEvent> getUpdateEventQueue() {
		return updateEventQueue;
	}
}
