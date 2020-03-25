/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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


import org.teamapps.universaldb.distribute.*;
import org.teamapps.universaldb.distribute.TransactionReader;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.file.FileStore;
import org.teamapps.universaldb.index.file.LocalFileStore;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.schema.SchemaInfoProvider;
import org.teamapps.universaldb.transaction.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class UniversalDB implements DataBaseMapper, TransactionIdProvider {

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
    private TransactionHead transactionHead;

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
		LocalFileStore fileStore = new LocalFileStore(new File(storagePath, "file-store"));
		return new UniversalDB(storagePath, schemaInfoProvider, fileStore);
	}

	public static UniversalDB createStandalone(File storagePath, SchemaInfoProvider schemaInfoProvider, FileStore fileStore) throws Exception {
		return new UniversalDB(storagePath, schemaInfoProvider, fileStore);
	}

	public static UniversalDB createClusterNode(File storagePath, SchemaInfoProvider schemaInfoProvider, ClusterSetConfig clusterConfig) throws Exception {
		LocalFileStore fileStore = new LocalFileStore(new File(storagePath, "file-store"));
		return new UniversalDB(storagePath, schemaInfoProvider, fileStore, clusterConfig);
	}


	private UniversalDB(File storagePath, SchemaInfoProvider schemaInfo, FileStore fileStore) throws Exception {
		this.storagePath = storagePath;
		this.transactionStore = new TransactionStore(storagePath);
		Transaction.setDataBase(this);

		Schema schema = Schema.parse(schemaInfo.getSchema());
		String pojoPath = schema.getPojoNamespace();
		this.schemaIndex = new SchemaIndex(Schema.parse(schema.getPojoNamespace()), storagePath);


		schemaIndex.setFileStore(fileStore);

		mapSchema(schema);

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
	}

	private void mapSchema(Schema schema) throws IOException {
		Schema localSchema = transactionStore.getSchema();
		if (localSchema != null) {
			if (!localSchema.isCompatibleWith(schema)) {
				throw new RuntimeException("Cannot load incompatible schema. Current schema is:\n" + schema + "\nNew schema is:\n" + localSchema);
			}
			localSchema.merge(schema);
			transactionStore.saveSchema(localSchema);
		} else {
			localSchema = schema;
			transactionStore.saveSchema(localSchema);
		}
		localSchema.mapSchema();
		schemaIndex.merge(localSchema);

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

		Schema schema = Schema.parse(schemaInfo.getSchema());
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

		clusterConfig.setProducerClientId(schemaStats.getClientId());
		clusterConfig.setConsumerGroupId(schemaStats.getGroupId());
		clusterConfig.setHeadProducerClientId(schemaStats.getHeadClientId());
		clusterConfig.setHeadConsumerGroupId(schemaStats.getHeadGroupId());

        transactionWriter = new TransactionWriter(clusterConfig);

        transactionReader = new TransactionReader(clusterConfig,
                this,
                transactionWriter.getTransactionMap(),
				this);

        transactionHead = new TransactionHead(clusterConfig,
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
		schemaIndex.merge(localSchema);

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

	public void executeTransaction(ClusterTransaction transaction) throws IOException {
		if (transactionWriter != null) {
			executeClusterTransaction(transaction);
		} else {
			executeStandaloneTransaction(transaction);
		}
	}

	private synchronized void executeStandaloneTransaction(ClusterTransaction transaction) throws IOException {
		TransactionRequest request = transaction.createRequest();
		transactionStore.executeTransaction(request);
	}

	private void executeClusterTransaction(ClusterTransaction transaction) throws IOException {
		TransactionExecutionResult transactionExecutionResult = transactionWriter.writeTransaction(transaction);
		transactionExecutionResult.waitForExecution();
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
    public long getNextTransactionId() {
        if (schemaStats != null) {
            return schemaStats.getNextTransactionId();
        } else {
            return transactionStore.getNextTransactionId();
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
}
