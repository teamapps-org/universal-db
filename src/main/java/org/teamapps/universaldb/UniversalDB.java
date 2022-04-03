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
import org.teamapps.universaldb.distribute.TransactionReader;
import org.teamapps.universaldb.distribute.*;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.file.FileStore;
import org.teamapps.universaldb.index.file.LocalFileStore;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.reference.value.MultiReferenceEditValue;
import org.teamapps.universaldb.index.reference.value.RecordReference;
import org.teamapps.universaldb.index.reference.value.ResolvedMultiReferenceUpdate;
import org.teamapps.universaldb.index.text.FullTextIndexValue;
import org.teamapps.universaldb.index.transaction.TransactionIndex;
import org.teamapps.universaldb.index.transaction.request.TransactionRequest2;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecord;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordType;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordValue;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransaction;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecord;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecordType;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecordValue;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.schema.*;
import org.teamapps.universaldb.transaction.*;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public class UniversalDB implements DataBaseMapper  { //TransactionIdHandler

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private static final ThreadLocal<Integer> THREAD_LOCAL_USER_ID = ThreadLocal.withInitial(() -> 0);
//	private static final ThreadLocal<Transaction> THREAD_LOCAL_TRANSACTION = new ThreadLocal<>();

	private final File storagePath;
	private final SchemaIndex schemaIndex;

//	private TransactionStore transactionStore;
//	private SchemaStats schemaStats;
//	private TransactionWriter transactionWriter;
//	private TransactionReader transactionReader;
//	private TransactionMaster transactionMaster;


	public static UniversalDB TEST_INSTANCE;
	private TransactionIndex transactionIndex;

	private final Map<Integer, DatabaseIndex> databaseById = new HashMap<>();
	private final Map<Integer, TableIndex> tableById = new HashMap<>();
	private final Map<Integer, ColumnIndex> columnById = new HashMap<>();
	private final Map<TableIndex, Class> entityClassByTableIndex = new HashMap<>();
	private final Map<TableIndex, Class> queryClassByTableIndex = new HashMap<>();
	private final Map<String, TableIndex> tableIndexByPath = new HashMap<>();

	public static int getUserId() {
		return THREAD_LOCAL_USER_ID.get();
	}

	public static void setUserId(int userId) {
		THREAD_LOCAL_USER_ID.set(userId);
	}

//	public static void startThreadLocalTransaction() {
//		THREAD_LOCAL_TRANSACTION.set(Transaction.create());
//	}

//	public static Transaction getThreadLocalTransaction() {
//		return THREAD_LOCAL_TRANSACTION.get();
//	}

//	public static void executeThreadLocalTransaction() {
//		Transaction transaction = THREAD_LOCAL_TRANSACTION.get();
//		transaction.execute();
//		THREAD_LOCAL_TRANSACTION.set(null);
//	}

	public static UniversalDB createStandalone(File storagePath, SchemaInfoProvider schemaInfoProvider) throws Exception {
		return createStandalone(storagePath, schemaInfoProvider, true);
	}

	public static UniversalDB createStandalone(File storagePath, SchemaInfoProvider schemaInfoProvider, boolean writeTransactionLog) throws Exception {
		LocalFileStore fileStore = new LocalFileStore(new File(storagePath, "file-store"));
		return new UniversalDB(storagePath, schemaInfoProvider, fileStore, writeTransactionLog);
	}

	public static UniversalDB createStandalone(File storagePath, SchemaInfoProvider schemaInfoProvider, FileStore fileStore, boolean writeTransactionLog) throws Exception {
		return new UniversalDB(storagePath, schemaInfoProvider, fileStore, writeTransactionLog);
	}

//	public static UniversalDB createClusterNode(File storagePath, SchemaInfoProvider schemaInfoProvider, ClusterSetConfig clusterConfig) throws Exception {
//		LocalFileStore fileStore = new LocalFileStore(new File(storagePath, "file-store"));
//		return new UniversalDB(storagePath, schemaInfoProvider, fileStore, clusterConfig);
//	}

	private UniversalDB(File storagePath, SchemaInfoProvider schemaInfo, FileStore fileStore, boolean writeTransactionLog) throws Exception {
		TEST_INSTANCE = this;
		this.storagePath = storagePath;
		this.transactionIndex = new TransactionIndex(storagePath);
//		this.transactionStore = new TransactionStore(storagePath, writeTransactionLog);
//		logger.info("SCHEMA MODEL UPDATES: " + transactionStore.getSchemaLogs().size());
//		Transaction.setDataBase(this);

		Schema schema = schemaInfo.getSchema();
		String pojoPath = schema.getPojoNamespace();
		this.schemaIndex = new SchemaIndex(schema, storagePath);

		schemaIndex.setFileStore(fileStore);

		mapSchema(schema);

		for (DatabaseIndex database : schemaIndex.getDatabases()) {
			String path = pojoPath + "." + database.getName().toLowerCase();
			for (TableIndex tableIndex : database.getTables()) {
				try {
					String tableName = tableIndex.getName();
					String className = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
					Class<?> schemaClass = Class.forName(className);
					Method method = schemaClass.getDeclaredMethod("setTableIndex", TableIndex.class);
					method.setAccessible(true);
					method.invoke(null, tableIndex);

					entityClassByTableIndex.put(tableIndex, schemaClass);
					tableIndexByPath.put(tableIndex.getFQN(), tableIndex);
					String queryClassName = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1) + "Query";
					Class<?> queryClass = Class.forName(queryClassName);
					queryClassByTableIndex.put(tableIndex, queryClass);

				} catch (ClassNotFoundException e) {
					logger.info("Could not load entity class for tableIndex:" + tableIndex.getFQN());
				}
			}
		}
		installTableViews(schemaInfo, UniversalDB.class.getClassLoader());
	}

	public void installTableViews(SchemaInfoProvider schemaInfo, ClassLoader classLoader) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Schema schema = schemaInfo.getSchema();
		String pojoPath = schema.getPojoNamespace();
		for (Database database : schema.getDatabases()) {
			String path = pojoPath + "." + database.getName().toLowerCase();
			for (Table table : database.getViewTables()) {
				TableIndex tableIndex = schemaIndex.getTableByPath(table.getReferencedTablePath());
				String tableName = table.getName();
				String className = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
				Class<?> schemaClass = Class.forName(className, true, classLoader);
				Method method = schemaClass.getDeclaredMethod("setTableIndex", TableIndex.class);
				method.setAccessible(true);
				method.invoke(null, tableIndex);
			}
		}
	}

	public void addAuxiliaryModel(SchemaInfoProvider schemaInfo, ClassLoader classLoader) throws IOException {
		Schema schema = schemaInfo.getSchema();
		Schema localSchema = transactionIndex.getCurrentSchema();
//		Schema localSchema = transactionStore.getSchema();
		String originalSchema = localSchema != null ? localSchema.getSchemaDefinition() : null;
		if (!localSchema.isCompatibleWith(schema)) {
			throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + schema + "\nNew schema is:\n" + localSchema);
		}
		checkSchemaUpdate(localSchema, schema);
		localSchema.merge(schema);
		localSchema.mapSchema();
		String updatedSchema = localSchema.getSchemaDefinition();
		boolean isSchemaUpdate = !Objects.equals(originalSchema, updatedSchema);
		if (isSchemaUpdate) {
			logger.info("SCHEMA UPDATE:\n" + originalSchema + "\nNew schema is:\n" + updatedSchema);
		}
		transactionIndex.updateSchema(localSchema);
//		transactionStore.saveSchema(localSchema, isSchemaUpdate);
		schemaIndex.merge(localSchema, false);

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

	public void installAuxiliaryModelClassed(SchemaInfoProvider schemaInfo, ClassLoader classLoader) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
		Schema schema = schemaInfo.getSchema();
		String pojoPath = schema.getPojoNamespace();

		for (Database database : schema.getDatabases()) {
			String path = pojoPath + "." + database.getName().toLowerCase();
			for (Table table : database.getTables()) {
				TableIndex tableIndex = schemaIndex.getTable(table);
				String tableName = table.getName();
				String className = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
				Class<?> schemaClass = Class.forName(className, true, classLoader);
				Method method = schemaClass.getDeclaredMethod("setTableIndex", TableIndex.class);
				method.setAccessible(true);
				method.invoke(null, tableIndex);

				entityClassByTableIndex.put(tableIndex, schemaClass);
				tableIndexByPath.put(tableIndex.getFQN(), tableIndex);
				String queryClassName = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1) + "Query";
				Class<?> queryClass = Class.forName(queryClassName, true, classLoader);
				queryClassByTableIndex.put(tableIndex, queryClass);
			}
		}
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

	public TableIndex addTable(Table table, String database) {
		DatabaseIndex db = schemaIndex.getDatabase(database);
		TableIndex tableIndex = new TableIndex(db, table, table.getTableConfig());
		tableIndex.setMappingId(table.getMappingId());
		tableIndex.merge(table);
		return tableIndex;
	}

	private void mapSchema(Schema schema) throws IOException {
		Schema localSchema = transactionIndex.getCurrentSchema();
//		Schema localSchema = transactionStore.getSchema();
		String originalSchema = localSchema != null ? localSchema.getSchemaDefinition() : null;
		if (localSchema != null) {
			if (!localSchema.isCompatibleWith(schema)) {
				throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + localSchema + "\nNew schema is:\n" + schema);
			}
			checkSchemaUpdate(localSchema, schema);
			localSchema.merge(schema);
		} else {
			localSchema = schema;
		}
		localSchema.mapSchema();
		String updatedSchema = localSchema.getSchemaDefinition();
		boolean isSchemaUpdate = !Objects.equals(originalSchema, updatedSchema);
		if (isSchemaUpdate) {
			logger.info("SCHEMA UPDATE:\n" + originalSchema + "\nNew schema is:\n" + updatedSchema);
		}
		transactionIndex.updateSchema(localSchema);
//		transactionStore.saveSchema(localSchema, isSchemaUpdate);
		schemaIndex.merge(localSchema, true);

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

	private void checkSchemaUpdate(Schema localSchema, Schema schemaUpdate) {
		Schema schemaCopy = Schema.parse(localSchema.getSchemaDefinition());
		if (!schemaCopy.isCompatibleWith(schemaUpdate)) {
			throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + localSchema + "\nNew schema is:\n" + schemaUpdate);
		}
		schemaCopy.merge(schemaUpdate);
		schemaCopy.mapSchema();
		if (!schemaCopy.checkModel()) {
			throw new RuntimeException("Schema merging leads to corrupted model:\n" + schemaCopy + "\nNew schema is:\n" + schemaUpdate);
		}
	}

	public synchronized TransactionRequest2 createTransactionRequest() {
		return new TransactionRequest2(transactionIndex.getNodeId(), transactionIndex.createTransactionRequestId(), getUserId());
	}

	public synchronized int executeTransaction(TransactionRequest2 transaction)  {

		//if not leader -> send to leader
		try {
			ResolvedTransaction resolvedTransaction = handleTransactionRequest(transaction);
			if (resolvedTransaction.getTransactionRecords().size() == 1) {
				return resolvedTransaction.getTransactionRecords().get(0).getRecordId();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}

	private synchronized ResolvedTransaction handleTransactionRequest(TransactionRequest2 transaction) throws Exception {
		//write TR to index -> create ResolvedTransaction (RT)
		//write transaction log (RT)
		//write table log (RT)
		//send to peers - if leader

		long transactionId = transactionIndex.getLastTransactionId() + 1;
		ResolvedTransaction resolvedTransaction = ResolvedTransaction.createFromRequest(transactionId, transaction);

//		Map<Integer, Integer> recordIdByCorrelationId = new HashMap<>();

		for (TransactionRequestRecord record : transaction.getRecords()) {
			if (record.getRecordType() == TransactionRequestRecordType.CREATE || record.getRecordType() == TransactionRequestRecordType.CREATE_WITH_ID) {
				TableIndex tableIndex = getTableIndexById(record.getTableId());
				int recordId = tableIndex.createRecord(record.getRecordId());
				transaction.putResolvedRecordIdForCorrelationId(record.getCorrelationId(), recordId);
			}
		}


		for (TransactionRequestRecord record : transaction.getRecords()) {
			TableIndex tableIndex = getTableIndexById(record.getTableId());
			if (record.isTransactionProcessingStarted()) {
				//make sure that a record that has been processed because of a reference ist not processed again
				logger.error("Prevented processing of record again:" + record.getTableId() + ":" + record.getRecordId());
				continue;
			}
			record.setTransactionProcessingStarted(true);

			int recordId = record.getRecordId() != 0 ? record.getRecordId() : transaction.getResolvedRecordIdByCorrelationId(record.getCorrelationId());
			ResolvedTransactionRecord resolvedRecord = ResolvedTransactionRecord.createFromRequest(record, recordId);
			resolvedTransaction.addTransactionRecord(resolvedRecord);

			switch (record.getRecordType()) {
				case CREATE:
				case CREATE_WITH_ID:
				case UPDATE:
					//tableIndex.setTransactionId(recordId, transactionId);

					for (TransactionRequestRecordValue recordValue : record.getRecordValues()) {
						List<CyclicReferenceUpdate> cyclicReferenceUpdates = persistColumnValueUpdates(recordId, recordValue, transaction.getRecordIdByCorrelationId(), resolvedRecord);
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
								return value.getIndexType() == IndexType.TEXT ? new FullTextIndexValue(columnName, (String) value.getValue()) :  new FullTextIndexValue(columnName, (TranslatableText) value.getValue());
							})
							.collect(Collectors.toList());
					if (!fullTextIndexValues.isEmpty()) {
						tableIndex.updateFullTextIndex(recordId, fullTextIndexValues, record.getRecordType() == TransactionRequestRecordType.UPDATE);
					}
					break;
				case DELETE:
					tableIndex.deleteRecord(record.getRecordId());
					for (TransactionRequestRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(recordId, recordValue, transaction.getRecordIdByCorrelationId(), resolvedRecord);
					}
					break;
				case RESTORE:
					tableIndex.restoreRecord(record.getRecordId());
					for (TransactionRequestRecordValue recordValue : record.getRecordValues()) {
						persistColumnValueUpdates(recordId, recordValue, transaction.getRecordIdByCorrelationId(), resolvedRecord);
					}
					break;
			}
		}
		transactionIndex.writeTransaction(resolvedTransaction);

		for (ResolvedTransactionRecord transactionRecord : resolvedTransaction.getTransactionRecords()) {
			TableIndex tableIndex = getTableIndexById(transactionRecord.getTableId());
			tableIndex.getRecordVersioningIndex().writeRecordUpdate(resolvedTransaction, transactionRecord);
		}

		return resolvedTransaction;
	}

	private synchronized void handleTransaction(ResolvedTransaction transaction) throws Exception {
		//only if peer:
			//write RT to index
			//write transaction log (RT)
			//write table log (RT)

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
								return value.getIndexType() == IndexType.TEXT ? new FullTextIndexValue(columnName, (String) value.getValue()) :  new FullTextIndexValue(columnName, (TranslatableText) value.getValue());
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

//	private UniversalDB(File storagePath, SchemaInfoProvider schemaInfo, FileStore fileStore, ClusterSetConfig clusterConfig) throws Exception {
//		this.storagePath = storagePath;
//		this.schemaStats = new SchemaStats(storagePath);
//		Transaction.setDataBase(this);
//
//		Schema schema = schemaInfo.getSchema();
//		String pojoPath = schema.getPojoNamespace();
//		this.schemaIndex = new SchemaIndex(Schema.parse(schema.getPojoNamespace()), storagePath);
//
//		schemaIndex.setFileStore(fileStore);
//
//		mapSchemaForCluster(schema);
//
//		for (DatabaseIndex database : schemaIndex.getDatabases()) {
//			String path = pojoPath + "." + database.getName().toLowerCase();
//			for (TableIndex table : database.getTables()) {
//				String tableName = table.getName();
//				String className = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
//				Class<?> schemaClass = Class.forName(className);
//				Method method = schemaClass.getDeclaredMethod("setTableIndex", TableIndex.class);
//				method.setAccessible(true);
//				method.invoke(null, table);
//			}
//		}
//
//		transactionWriter = new TransactionWriter(clusterConfig, schemaStats);
//
//		transactionReader = new TransactionReader(clusterConfig,
//				schemaStats,
//				this,
//				transactionWriter.getTransactionMap(),
//				this);
//
//		transactionMaster = new TransactionMaster(clusterConfig,
//				schemaStats,
//				this,
//				this);
//
//	}

//	//todo this should be merged with standalone db merging
//	private void mapSchemaForCluster(Schema schema) throws IOException {
//		Schema localSchema = schemaStats.getSchema();
//		if (localSchema != null) {
//			if (!localSchema.isCompatibleWith(schema)) {
//				throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + localSchema + "\nNew schema is:\n" + schema);
//			}
//			localSchema.merge(schema);
//			schemaStats.saveSchema(localSchema);
//		} else {
//			localSchema = schema;
//			schemaStats.saveSchema(localSchema);
//		}
//		localSchema.mapSchema();
//		schemaIndex.merge(localSchema, true);
//
//		for (DatabaseIndex database : schemaIndex.getDatabases()) {
//			databaseById.put(database.getMappingId(), database);
//			for (TableIndex table : database.getTables()) {
//				tableById.put(table.getMappingId(), table);
//				for (ColumnIndex columnIndex : table.getColumnIndices()) {
//					columnById.put(columnIndex.getMappingId(), columnIndex);
//				}
//			}
//		}
//	}
//
//	public void executeTransaction(ClusterTransaction transaction, boolean asynchronous) throws IOException {
//		if (transactionWriter != null) {
//			executeClusterTransaction(transaction, asynchronous);
//		} else {
//			executeStandaloneTransaction(transaction);
//		}
//	}
//
//	private synchronized void executeStandaloneTransaction(ClusterTransaction transaction) throws IOException {
//		TransactionRequest request = transaction.createRequest();
//		transactionStore.executeTransaction(request);
//	}
//
//	private void executeClusterTransaction(ClusterTransaction transaction, boolean asynchronous) throws IOException {
//		TransactionExecutionResult transactionExecutionResult = transactionWriter.writeTransaction(transaction);
//		if (!asynchronous) {
//			transactionExecutionResult.waitForExecution();
//		}
//	}

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

//	@Override
//	public long getAndCommitNextTransactionId() {
//		if (schemaStats != null) {
//			return schemaStats.getAndCommitNextTransactionId();
//		} else {
//			return transactionStore.getAndCommitNextTransactionId();
//		}
//	}
//
//	@Override
//	public long getLastCommittedTransactionId() {
//		if (schemaStats != null) {
//			return schemaStats.getLastCommittedTransactionId();
//		} else {
//			return transactionStore.getLastCommittedTransactionId();
//		}
//	}
//
//	@Override
//	public void commitTransactionId(long id) {
//		if (schemaStats != null) {
//			schemaStats.commitTransactionId(id);
//		}
//	}
//
//	public SchemaStats getClusterSchemaStats() {
//		return schemaStats;
//	}

	public SchemaIndex getSchemaIndex() {
		return schemaIndex;
	}
}
