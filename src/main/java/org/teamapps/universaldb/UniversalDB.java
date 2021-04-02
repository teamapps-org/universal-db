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
package org.teamapps.universaldb;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.distribute.TransactionReader;
import org.teamapps.universaldb.distribute.*;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.file.FileStore;
import org.teamapps.universaldb.index.file.LocalFileStore;
import org.teamapps.universaldb.schema.*;
import org.teamapps.universaldb.transaction.*;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class UniversalDB implements DataBaseMapper, TransactionIdHandler {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private static final ThreadLocal<Integer> THREAD_LOCAL_USER_ID = ThreadLocal.withInitial(() -> 0);
	private static final ThreadLocal<Transaction> THREAD_LOCAL_TRANSACTION = new ThreadLocal<>();

	private final File storagePath;
	private final Map<Integer, DatabaseIndex> databaseById = new HashMap<>();
	private final Map<Integer, TableIndex> tableById = new HashMap<>();
	private final Map<Integer, ColumnIndex> columnById = new HashMap<>();
	private final SchemaIndex schemaIndex;

	private TransactionStore transactionStore;
	private SchemaStats schemaStats;
	private TransactionWriter transactionWriter;
	private TransactionReader transactionReader;
	private TransactionMaster transactionMaster;
	private final Map<TableIndex, Class> entityClassByTableIndex = new HashMap<>();
	private final Map<TableIndex, Class> queryClassByTableIndex = new HashMap<>();
	private final Map<String, TableIndex> tableIndexByPath = new HashMap<>();

	public static int getUserId() {
		return THREAD_LOCAL_USER_ID.get();
	}

	public static void setUserId(int userId) {
		THREAD_LOCAL_USER_ID.set(userId);
	}

	public static void startThreadLocalTransaction() {
		THREAD_LOCAL_TRANSACTION.set(Transaction.create());
	}

	public static Transaction getThreadLocalTransaction() {
		return THREAD_LOCAL_TRANSACTION.get();
	}

	public static void executeThreadLocalTransaction() {
		Transaction transaction = THREAD_LOCAL_TRANSACTION.get();
		transaction.execute();
		THREAD_LOCAL_TRANSACTION.set(null);
	}

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

	public static UniversalDB createClusterNode(File storagePath, SchemaInfoProvider schemaInfoProvider, ClusterSetConfig clusterConfig) throws Exception {
		LocalFileStore fileStore = new LocalFileStore(new File(storagePath, "file-store"));
		return new UniversalDB(storagePath, schemaInfoProvider, fileStore, clusterConfig);
	}


	private UniversalDB(File storagePath, SchemaInfoProvider schemaInfo, FileStore fileStore, boolean writeTransactionLog) throws Exception {
		this.storagePath = storagePath;
		this.transactionStore = new TransactionStore(storagePath, writeTransactionLog);
		Transaction.setDataBase(this);

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
		Schema localSchema = transactionStore.getSchema();
		if (!localSchema.isCompatibleWith(schema)) {
			throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + schema + "\nNew schema is:\n" + localSchema);
		}
		localSchema.merge(schema);
		localSchema.mapSchema();
		transactionStore.saveSchema(localSchema);
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
		Schema localSchema = transactionStore.getSchema();
		if (localSchema != null) {
			if (!localSchema.isCompatibleWith(schema)) {
				throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + schema + "\nNew schema is:\n" + localSchema);
			}
			localSchema.merge(schema);
		} else {
			localSchema = schema;
		}
		localSchema.mapSchema();
		transactionStore.saveSchema(localSchema);
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

	private UniversalDB(File storagePath, SchemaInfoProvider schemaInfo, FileStore fileStore, ClusterSetConfig clusterConfig) throws Exception {
		this.storagePath = storagePath;
		this.schemaStats = new SchemaStats(storagePath);
		Transaction.setDataBase(this);

		Schema schema = schemaInfo.getSchema();
		String pojoPath = schema.getPojoNamespace();
		this.schemaIndex = new SchemaIndex(Schema.parse(schema.getPojoNamespace()), storagePath);

		schemaIndex.setFileStore(fileStore);

		mapSchemaForCluster(schema);

		for (DatabaseIndex database : schemaIndex.getDatabases()) {
			String path = pojoPath + "." + database.getName().toLowerCase();
			for (TableIndex table : database.getTables()) {
				String tableName = table.getName();
				String className = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
				Class<?> schemaClass = Class.forName(className);
				Method method = schemaClass.getDeclaredMethod("setTableIndex", TableIndex.class);
				method.setAccessible(true);
				method.invoke(null, table);
			}
		}

		transactionWriter = new TransactionWriter(clusterConfig, schemaStats);

		transactionReader = new TransactionReader(clusterConfig,
				schemaStats,
				this,
				transactionWriter.getTransactionMap(),
				this);

		transactionMaster = new TransactionMaster(clusterConfig,
				schemaStats,
				this,
				this);

	}

	//todo this should be merged with standalone db merging
	private void mapSchemaForCluster(Schema schema) throws IOException {
		Schema localSchema = schemaStats.getSchema();
		if (localSchema != null) {
			if (!localSchema.isCompatibleWith(schema)) {
				throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + schema + "\nNew schema is:\n" + localSchema);
			}
			localSchema.merge(schema);
			schemaStats.saveSchema(localSchema);
		} else {
			localSchema = schema;
			schemaStats.saveSchema(localSchema);
		}
		localSchema.mapSchema();
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

	public void executeTransaction(ClusterTransaction transaction, boolean asynchronous) throws IOException {
		if (transactionWriter != null) {
			executeClusterTransaction(transaction, asynchronous);
		} else {
			executeStandaloneTransaction(transaction);
		}
	}

	private synchronized void executeStandaloneTransaction(ClusterTransaction transaction) throws IOException {
		TransactionRequest request = transaction.createRequest();
		transactionStore.executeTransaction(request);
	}

	private void executeClusterTransaction(ClusterTransaction transaction, boolean asynchronous) throws IOException {
		TransactionExecutionResult transactionExecutionResult = transactionWriter.writeTransaction(transaction);
		if (!asynchronous) {
			transactionExecutionResult.waitForExecution();
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

	@Override
	public DatabaseIndex getDatabaseById(int mappingId) {
		return databaseById.get(mappingId);
	}

	@Override
	public TableIndex getCollectionIndexById(int mappingId) {
		return tableById.get(mappingId);
	}

	@Override
	public ColumnIndex getColumnById(int mappingId) {
		return columnById.get(mappingId);
	}

	@Override
	public long getAndCommitNextTransactionId() {
		if (schemaStats != null) {
			return schemaStats.getAndCommitNextTransactionId();
		} else {
			return transactionStore.getAndCommitNextTransactionId();
		}
	}

	@Override
	public long getLastCommittedTransactionId() {
		if (schemaStats != null) {
			return schemaStats.getLastCommittedTransactionId();
		} else {
			return transactionStore.getLastCommittedTransactionId();
		}
	}

	@Override
	public void commitTransactionId(long id) {
		if (schemaStats != null) {
			schemaStats.commitTransactionId(id);
		}
	}

	public SchemaStats getClusterSchemaStats() {
		return schemaStats;
	}

	public SchemaIndex getSchemaIndex() {
		return schemaIndex;
	}
}
