/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.DatabaseIndex;
import org.teamapps.universaldb.index.FieldIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.counter.ViewCounter;
import org.teamapps.universaldb.index.counter.ViewCounterImpl;
import org.teamapps.universaldb.index.file.FileIndex;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.file.store.DatabaseFileStore;
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
import org.teamapps.universaldb.index.transaction.schema.ModelUpdate;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.model.DatabaseModel;
import org.teamapps.universaldb.model.TableModel;
import org.teamapps.universaldb.schema.ModelProvider;
import org.teamapps.universaldb.schema.Table;
import org.teamapps.universaldb.update.RecordUpdateEvent;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class UniversalDB {
	public static final Marker SKIP_DB_LOGGING = MarkerFactory.getMarker("SKIP_DB_LOGGING");

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private static final ThreadLocal<Integer> THREAD_LOCAL_USER_ID = ThreadLocal.withInitial(() -> 0);
	private static final ThreadLocal<UserContext> THREAD_LOCAL_USER_CONTEXT = ThreadLocal.withInitial(() -> UserContext.create(Locale.US));

	private final DatabaseManager databaseManager;
	private final DatabaseIndex databaseIndex;
	private final DatabaseFileStore fileStore;
	private final File indexPath;
	private final File fullTextIndexPath;
	private final File transactionLogPath;
	private final TransactionIndex transactionIndex;

	private final Map<Integer, TableIndex> tableById = new HashMap<>();
	private final Map<Integer, FieldIndex> columnById = new HashMap<>();
	private final Map<TableIndex, Class> entityClassByTableIndex = new HashMap<>();
	private final Map<TableIndex, Class> queryClassByTableIndex = new HashMap<>();
	private final ArrayBlockingQueue<RecordUpdateEvent> updateEventQueue = new ArrayBlockingQueue<>(25_000);
	private final Map<Long, CompletableFuture<ResolvedTransaction>> transactionCompletableFutureMap = new ConcurrentHashMap<>();
	private final Map<TableIndex, ViewCounter> viewCounterMap = new ConcurrentHashMap<>();

	protected UniversalDB(ModelProvider modelProvider, DatabaseManager databaseManager, DatabaseFileStore fileStore, File indexPath, File fullTextIndexPath, File transactionLogPath, ClassLoader classLoader, boolean skipTransactionIndexCheck) throws Exception {
		this.databaseManager = databaseManager;
		this.fileStore = fileStore;
		this.indexPath = indexPath;
		this.fullTextIndexPath = fullTextIndexPath;
		this.transactionLogPath = transactionLogPath;
		this.transactionIndex = new TransactionIndex(transactionLogPath, skipTransactionIndexCheck);
		createShutdownHook();
		DatabaseModel model = modelProvider.getModel();
		if (!model.isValid()) {
			throw new RuntimeException("Error invalid database model:" + model.getName());
		}

		if (!transactionIndex.isValidModel(model)) {
			if (transactionIndex.getCurrentModel() != null) {
				List<String> errors = transactionIndex.getCurrentModel().checkCompatibilityErrors(model);
				logger.error("Model errors: " + String.join("\n", errors));
			}
			throw new RuntimeException("Cannot load incompatible model. Current model is:\n" + transactionIndex.getCurrentModel() + "\nNew model is:\n" + model);
		}

		databaseIndex = new DatabaseIndex(this, model.getName(), indexPath, fullTextIndexPath, fileStore);

		if (transactionIndex.isModelUpdate(model)) {
			executeTransaction(createModelUpdateTransactionRequest(model));
		} else {
			DatabaseModel currentModel = transactionIndex.getCurrentModel();
			mergeDatabaseIndex(currentModel);
		}

		installLocalTableClasses(classLoader);
		databaseManager.registerDatabase(model.getName(), this, classLoader);
	}

	public static int getUserId() {
		return THREAD_LOCAL_USER_ID.get();
	}

	public static void setUserId(int userId) {
		THREAD_LOCAL_USER_ID.set(userId);
	}

	public static UserContext getUserRankedLanguages() {
		UserContext context = THREAD_LOCAL_USER_CONTEXT.get();
		return context != null ? context : UserContext.create(Locale.US);
	}

	public static void setUserRankedLanguages(UserContext context) {
		THREAD_LOCAL_USER_CONTEXT.set(context);
	}

	private void mergeDatabaseIndex(DatabaseModel currentModel) {
		databaseIndex.installModel(currentModel, true, this);
		for (TableIndex table : databaseIndex.getTables()) {
			tableById.put(table.getMappingId(), table);
			for (FieldIndex fieldIndex : table.getFieldIndices()) {
				columnById.put(fieldIndex.getMappingId(), fieldIndex);
			}
		}
	}


//	public UniversalDB(File storagePath, LogIterator logIterator) throws Exception {
//		this.storagePath = storagePath;
//		this.transactionIndex = new TransactionIndex(storagePath);
//		LocalFileStore fileStore = new LocalFileStore(new File(storagePath, "file-store"));
//		createShutdownHook();
//
//		Schema schema = new Schema();
//		this.schemaIndex = new SchemaIndex(schema, storagePath);
//		this.schemaIndex.setFileStore(fileStore);
//
//		long time = System.currentTimeMillis();
//		long count = 0;
//		while (logIterator.hasNext()) {
//			byte[] bytes = logIterator.next();
//			ResolvedTransaction transaction = ResolvedTransaction.createResolvedTransaction(bytes);
//			handleTransaction(transaction);
//			count++;
//		}
//		logger.info("Imported " + count + " transactions in: " + (System.currentTimeMillis() - time));
//	}

	private void createShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				logger.info(SKIP_DB_LOGGING, "SHUTTING DOWN DATABASE");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}));
	}

	public synchronized ViewCounter getOrCreateViewCounter(TableIndex tableIndex) {
		ViewCounter viewCounter = viewCounterMap.get(tableIndex);
		if (viewCounter == null) {
			viewCounter = new ViewCounterImpl(tableIndex);
			viewCounterMap.put(tableIndex, viewCounter);
		}
		return viewCounter;
	}

	private void installLocalTableClasses(ClassLoader classLoader) throws Exception {
		DatabaseModel currentModel = transactionIndex.getCurrentModel();
		for (TableModel tableModel : currentModel.getLocalTables()) {
			TableIndex tableIndex = databaseIndex.getTable(tableModel.getName());
			installTablePojos(classLoader, currentModel.getFullNameSpace(), tableModel, tableIndex);
		}
	}

	public void installAvailableRemoteTables(ClassLoader localDbClassLoader) {
		try {
			databaseIndex.installAvailableRemoteReferences(databaseManager);
			DatabaseModel currentModel = transactionIndex.getCurrentModel();
			for (TableModel remoteTable : currentModel.getRemoteTables()) {
				UniversalDB remoteDb = databaseManager.getDatabase(remoteTable.getRemoteDatabase());
				if (remoteDb != null) {
					ClassLoader remoteDbClassLoader = databaseManager.getClassLoader(remoteTable.getRemoteDatabase());
					TableIndex tableIndex = remoteDb.getDatabaseIndex().getTable(remoteTable.getRemoteTableName());
					if (tableIndex != null) {
						String fullNameSpace = remoteTable.getRemoteDatabaseNamespace() != null ? remoteTable.getRemoteDatabaseNamespace() + "." + remoteTable.getRemoteDatabase().toLowerCase() : currentModel.getFullNameSpace();
						ClassLoader classLoader = remoteTable.getRemoteDatabaseNamespace() != null ? remoteDbClassLoader : localDbClassLoader;
						installTablePojos(classLoader, fullNameSpace, remoteTable, tableIndex);
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void installTablePojos(ClassLoader classLoader, String fullNamespace, TableModel tableModel, TableIndex tableIndex) throws Exception {
		String tableName = tableModel.getName();
		try {
			String className = fullNamespace + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
			Class<?> schemaClass = Class.forName(className, true, classLoader);
			Method method = schemaClass.getDeclaredMethod("setTableIndex", TableIndex.class, UniversalDB.class);
			method.setAccessible(true);
			method.invoke(null, tableIndex, this);

			String queryClassName = fullNamespace + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1) + "Query";
			Class<?> queryClass = Class.forName(queryClassName, true, classLoader);
			entityClassByTableIndex.put(tableIndex, schemaClass);
			queryClassByTableIndex.put(tableIndex, queryClass);
		} catch (ClassNotFoundException e) {
			logger.warn("Could not load entity class for tableIndex:" + tableIndex.getFQN() + ", " + e.getMessage());
		} catch (Exception e) {
			throw e;
		}
	}

	public void installModelUpdate(ModelProvider modelProvider, ClassLoader classLoader) throws Exception {
		DatabaseModel model = modelProvider.getModel();
		if (!transactionIndex.isValidModel(model)) {
			if (transactionIndex.getCurrentModel() != null) {
				List<String> errors = transactionIndex.getCurrentModel().checkCompatibilityErrors(model);
				logger.error("Model errors: " + String.join("\n", errors));
			}
			throw new RuntimeException("Cannot load incompatible model. Current model is:\n" + transactionIndex.getCurrentModel() + "\nNew model is:\n" + model);
		}
		if (transactionIndex.isModelUpdate(model)) {
			TransactionRequest modelUpdateTransactionRequest = createModelUpdateTransactionRequest(model);
			executeTransaction(modelUpdateTransactionRequest);
		}
		installLocalTableClasses(classLoader);
		databaseManager.updateDatabase(getName(), classLoader);
	}

	public Class getEntityClass(TableIndex tableIndex) {
		return entityClassByTableIndex.get(tableIndex);
	}

	public Class getQueryClass(TableIndex tableIndex) {
		return queryClassByTableIndex.get(tableIndex);
	}

	public synchronized TransactionRequest createTransactionRequest() {
		return new TransactionRequest(transactionIndex.getNodeId(), transactionIndex.createTransactionRequestId(), getUserId());
	}

	public synchronized TransactionRequest createTransactionRequest(int userId, long timestamp) {
		return new TransactionRequest(transactionIndex.getNodeId(), transactionIndex.createTransactionRequestId(), userId, timestamp);
	}

	public synchronized TransactionRequest createModelUpdateTransactionRequest(DatabaseModel databaseModel) {
		return new TransactionRequest(transactionIndex.getNodeId(), transactionIndex.createTransactionRequestId(), getUserId(), databaseModel);
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

	private void writeInitialTransaction(TableIndex tableIndex, int recordId, boolean deleted) throws Exception {
		ResolvedTransaction transaction = createInitialTransaction(tableIndex, recordId, false);
		ResolvedTransactionRecord record = new ResolvedTransactionRecord(ResolvedTransactionRecordType.CREATE_WITH_ID, tableIndex.getMappingId(), recordId);
		transaction.addTransactionRecord(record);
		List<FieldIndex> columnIndices = tableIndex.getFieldIndices().stream().filter(col -> !col.isEmpty(recordId)).toList();
		for (FieldIndex column : columnIndices) {
			ResolvedTransactionRecordValue recordValue = createInitialTransactionRecordValue(column, recordId);
			record.addRecordValue(recordValue);
		}
		tableIndex.getRecordVersioningIndex().writeRecordUpdate(transaction, record);
		transactionIndex.writeTransaction(transaction);
		if (deleted) {
			transaction = createInitialTransaction(tableIndex, recordId, true);
			record = new ResolvedTransactionRecord(ResolvedTransactionRecordType.DELETE, tableIndex.getMappingId(), recordId);
			transaction.addTransactionRecord(record);
			record.addRecordValue(createInitialTransactionRecordValue(tableIndex.getFieldIndex(Table.FIELD_DELETION_DATE), recordId));
			record.addRecordValue(createInitialTransactionRecordValue(tableIndex.getFieldIndex(Table.FIELD_DELETED_BY), recordId));
			tableIndex.getRecordVersioningIndex().writeRecordUpdate(transaction, record);
			transactionIndex.writeTransaction(transaction);
		}
	}

	private ResolvedTransaction createInitialTransaction(TableIndex tableIndex, int recordId, boolean deleted) {
		long transactionId = transactionIndex.getLastTransactionId() + 1;
		int userId = 0;
		int timestamp = 0;
		FieldIndex dateColumn = tableIndex.getFieldIndex(deleted ? Table.FIELD_DELETION_DATE : Table.FIELD_CREATION_DATE);
		FieldIndex userRefColumn = tableIndex.getFieldIndex(deleted ? Table.FIELD_DELETED_BY : Table.FIELD_CREATED_BY);
		if (dateColumn != null && userRefColumn != null) {
			userId = (int) userRefColumn.getGenericValue(recordId);
			timestamp = (int) dateColumn.getGenericValue(recordId);
		}
		return new ResolvedTransaction(transactionIndex.getNodeId(), transactionIndex.createTransactionRequestId(), transactionId, userId, timestamp * 1_000L);
	}

	private ResolvedTransactionRecordValue createInitialTransactionRecordValue(FieldIndex column, int recordId) {
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

	public ResolvedTransaction executeTransaction(TransactionRequest transaction) {
		try {
//			if (clusterClientTopic != null) {
//				if (!active) {
//					return null;
//				}
//				if (transaction.getTransactionType() == TransactionType.MODEL_UPDATE) {
//					return null;
//				} else {
//					CompletableFuture<ResolvedTransaction> completableFuture = new CompletableFuture<>();
//					transactionCompletableFutureMap.put(transaction.getRequestId(), completableFuture);
//					clusterClientTopic.sendMessageAsync(transaction.getBytes());
//					ResolvedTransaction resolvedTransaction = completableFuture.get();
//					resolvedTransaction.getRecordIdByCorrelationId().entrySet().forEach(entry -> transaction.putResolvedRecordIdForCorrelationId(entry.getKey(), entry.getValue()));
//					return resolvedTransaction;
//				}
//			} else {
//				return handleTransactionRequest(transaction);
//			}
			return handleTransactionRequest(transaction);
		} catch (Exception e) {
			throw new RuntimeException(e);
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
//		if (leaderTransactionClusterMessageQueue != null) {
//			leaderTransactionClusterMessageQueue.sendMessageAsync(resolvedTransaction.getBytes());
//		}
		return resolvedTransaction;
	}

	private void handleModelUpdateRequest(TransactionRequest request, ResolvedTransaction resolvedTransaction) throws Exception {
		DatabaseModel model = request.getDatabaseModel();
		if (!transactionIndex.isValidModel(model)) {
			throw new RuntimeException("Cannot update incompatible model. Current model is:\n" + transactionIndex.getCurrentModel() + "\nNew model is:\n" + model);
		}
		ModelUpdate modelUpdate = resolvedTransaction.getModelUpdate();
		transactionIndex.writeTransaction(resolvedTransaction);
		transactionIndex.writeModelUpdate(modelUpdate);
		mergeDatabaseIndex(modelUpdate.getMergedModel());
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
				case CREATE, CREATE_WITH_ID, UPDATE -> {
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
				}
				case DELETE -> {
					List<CyclicReferenceUpdate> cyclicReferenceUpdates = tableIndex.deleteRecord(record.getRecordId());
					for (TransactionRequestRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(recordId, recordValue, request.getRecordIdByCorrelationId(), resolvedRecord);
					}
					if (cyclicReferenceUpdates != null && !cyclicReferenceUpdates.isEmpty()) {
						for (CyclicReferenceUpdate referenceUpdate : cyclicReferenceUpdates) {
							resolvedTransaction.addTransactionRecord(ResolvedTransactionRecord.createCyclicRecord(referenceUpdate));
						}
					}
				}
				case RESTORE -> {
					List<CyclicReferenceUpdate> cyclicReferenceUpdates = tableIndex.restoreRecord(record.getRecordId());
					for (TransactionRequestRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(recordId, recordValue, request.getRecordIdByCorrelationId(), resolvedRecord);
					}
					if (cyclicReferenceUpdates != null && !cyclicReferenceUpdates.isEmpty()) {
						for (CyclicReferenceUpdate referenceUpdate : cyclicReferenceUpdates) {
							resolvedTransaction.addTransactionRecord(ResolvedTransactionRecord.createCyclicRecord(referenceUpdate));
						}
					}
				}
			}
			addRecordUpdateEvent(resolvedRecord, resolvedTransaction.getUserId());
		}
		transactionIndex.writeTransaction(resolvedTransaction);

		for (ResolvedTransactionRecord transactionRecord : resolvedTransaction.getTransactionRecords()) {
			TableIndex tableIndex = getTableIndexById(transactionRecord.getTableId());
			if (tableIndex.getTableModel().isVersioning()) {
				tableIndex.getRecordVersioningIndex().writeRecordUpdate(resolvedTransaction, transactionRecord);
			}
		}
		resolvedTransaction.setRecordIdByCorrelationId(request.getRecordIdByCorrelationId());
	}

	public synchronized void handleTransaction(ResolvedTransaction transaction) throws Exception {
		if (transaction.getTransactionType() == TransactionType.DATA_UPDATE) {
			handleDataUpdateTransaction(transaction);
		} else {
			handleModelUpdateTransaction(transaction);
		}
	}

	private void handleModelUpdateTransaction(ResolvedTransaction transaction) throws Exception {
		DatabaseModel model = transaction.getModelUpdate().getMergedModel();
		DatabaseModel currentModel = transactionIndex.getCurrentModel();
		if (currentModel != null) {
			currentModel.mergeModel(model);
		} else {
			currentModel = model;
		}
		transactionIndex.writeTransaction(transaction);

		transactionIndex.writeModelUpdate(transaction.getModelUpdate());
		mergeDatabaseIndex(currentModel);
	}

	private void handleDataUpdateTransaction(ResolvedTransaction transaction) throws Exception {
		for (ResolvedTransactionRecord record : transaction.getTransactionRecords()) {
			TableIndex tableIndex = getTableIndexById(record.getTableId());

			switch (record.getRecordType()) {
				case CREATE, CREATE_WITH_ID, UPDATE -> {
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
				}
				case DELETE -> {
					tableIndex.deleteRecord(record.getRecordId());
					for (ResolvedTransactionRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(record.getRecordId(), recordValue);
					}
				}
				case RESTORE -> {
					tableIndex.restoreRecord(record.getRecordId());
					for (ResolvedTransactionRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(record.getRecordId(), recordValue);
					}
				}
//				case ADD_CYCLIC_REFERENCE:
//					break;
//				case REMOVE_CYCLIC_REFERENCE:
//					break;
			}
			addRecordUpdateEvent(record, transaction.getUserId());
		}
		transactionIndex.writeTransaction(transaction);

		for (ResolvedTransactionRecord transactionRecord : transaction.getTransactionRecords()) {
			TableIndex tableIndex = getTableIndexById(transactionRecord.getTableId());
			if (tableIndex.getTableModel().isVersioning()) {
				tableIndex.getRecordVersioningIndex().writeRecordUpdate(transaction, transactionRecord);
			}
		}
	}

	private List<CyclicReferenceUpdate> persistColumnValueUpdates(int recordId, TransactionRequestRecordValue recordValue, Map<Integer, Integer> recordIdByCorrelationId, ResolvedTransactionRecord resolvedRecord) {
		FieldIndex fieldIndex = getColumnById(recordValue.getColumnId());
		Object value = recordValue.getValue();
		if (fieldIndex.getType() == IndexType.MULTI_REFERENCE) {
			MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) fieldIndex;
			MultiReferenceEditValue editValue = (MultiReferenceEditValue) value;
			editValue.updateReferences(recordIdByCorrelationId);
			ResolvedMultiReferenceUpdate resolvedUpdateValue = editValue.getResolvedUpdateValue();
			resolvedRecord.addRecordValue(new ResolvedTransactionRecordValue(recordValue.getColumnId(), recordValue.getIndexType(), resolvedUpdateValue));
			return multiReferenceIndex.setReferenceEditValue(recordId, editValue);
		} else if (fieldIndex.getType() == IndexType.REFERENCE) {
			SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) fieldIndex;
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
			fieldIndex.setGenericValue(recordId, value);
			resolvedRecord.addRecordValue(new ResolvedTransactionRecordValue(recordValue.getColumnId(), recordValue.getIndexType(), value));
		}
		return null;
	}

	private void persistColumnValueUpdates(int recordId, ResolvedTransactionRecordValue recordValue) {
		FieldIndex fieldIndex = getColumnById(recordValue.getColumnId());
		Object value = recordValue.getValue();
		if (fieldIndex.getType() == IndexType.MULTI_REFERENCE) {
			MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) fieldIndex;
			ResolvedMultiReferenceUpdate multiReferenceUpdate = (ResolvedMultiReferenceUpdate) value;
			multiReferenceIndex.setResolvedReferenceEditValue(recordId, multiReferenceUpdate);
		} else if (fieldIndex.getType() == IndexType.REFERENCE) {
			SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) fieldIndex;
			if (value != null) {
				int referencedRecordId = (int) value;
				singleReferenceIndex.setValue(recordId, referencedRecordId, false);
			} else {
				singleReferenceIndex.setValue(recordId, 0, false);
			}
		} else {
			fieldIndex.setGenericValue(recordId, value);
		}
	}

	public void createDatabaseDump(File dumpFolder) throws IOException {
		File dbFolder = new File(dumpFolder, databaseIndex.getName());
		dbFolder.mkdir();
		for (TableIndex table : databaseIndex.getTables()) {
			File tableFolder = new File(dbFolder, table.getName());
			tableFolder.mkdir();
			BitSet records = table.getRecords();
			for (FieldIndex fieldIndex : table.getFieldIndices()) {
				File dumpFile = new File(tableFolder, fieldIndex.getName() + ".dbd");
				fieldIndex.dumpIndex(dumpFile, records); //todo catch, continue and rethrow?
			}
		}
	}

	private void addRecordUpdateEvent(ResolvedTransactionRecord resolvedRecord, int userId) {
		if (userId > 0) {
			RecordUpdateEvent updateEvent = new RecordUpdateEvent(resolvedRecord.getTableId(), resolvedRecord.getRecordId(), userId, resolvedRecord.getRecordType().getUpdateType());
			updateEventQueue.offer(updateEvent);
		}
	}


	public TableIndex getTableIndexById(int mappingId) {
		return tableById.get(mappingId);
	}

	public FieldIndex getColumnById(int mappingId) {
		return columnById.get(mappingId);
	}

	public String getName() {
		return databaseIndex.getName();
	}

	public DatabaseIndex getDatabaseIndex() {
		return databaseIndex;
	}

	public TransactionIndex getTransactionIndex() {
		return transactionIndex;
	}

	public ArrayBlockingQueue<RecordUpdateEvent> getUpdateEventQueue() {
		return updateEventQueue;
	}
}
