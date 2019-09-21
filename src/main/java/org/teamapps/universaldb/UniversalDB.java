/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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


import org.teamapps.universaldb.cluster.Cluster;
import org.teamapps.universaldb.cluster.ClusterConfig;
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
import java.util.Iterator;
import java.util.Map;

public class UniversalDB implements DataBaseMapper, TransactionHandler {

    private static final ThreadLocal<Integer> THREAD_LOCAL_USER_ID = ThreadLocal.withInitial(() -> 0);
    private static final ThreadLocal<Transaction> THREAD_LOCAL_TRANSACTION = new ThreadLocal<>();

    private final File storagePath;
    private final TransactionStore transactionStore;
    private final Map<Integer, DatabaseIndex> databaseById = new HashMap<>();
    private final Map<Integer, TableIndex> tableById = new HashMap<>();
    private final Map<Integer, ColumnIndex> columnById = new HashMap<>();
    private final SchemaIndex schemaIndex;
    private Cluster cluster;

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
        return new UniversalDB(storagePath, schemaInfoProvider, fileStore,null);
    }

    public static UniversalDB createStandalone(File storagePath, Schema schema) throws IOException {
        return new UniversalDB(storagePath, schema, null);
    }

    public static UniversalDB createClusterNode(File storagePath, Schema schema, ClusterConfig clusterConfig) throws IOException {
        return new UniversalDB(storagePath, schema, clusterConfig);
    }

    public static UniversalDB createClusterNode(File storagePath, ClusterConfig clusterConfig) throws IOException {
        return new UniversalDB(storagePath, new Schema(), clusterConfig);
    }

    public UniversalDB(File storagePath, SchemaInfoProvider schemaInfo, FileStore fileStore, ClusterConfig clusterConfig) throws Exception {
        this.storagePath = storagePath;
        this.transactionStore = new TransactionStore(storagePath);
        Transaction.setDataBase(this);

        Schema schema = Schema.parse(schemaInfo.getSchema());
        String pojoPath = schema.getPojoNamespace();
        this.schemaIndex = new SchemaIndex(new Schema(schema.getPojoNamespace()), storagePath);


        schemaIndex.setFileStore(fileStore);

        mapSchema(schema);

        for (DatabaseIndex database : schemaIndex.getDatabases()) {
            String path = pojoPath + "." + database.getName().toLowerCase();
            for (TableIndex table : database.getTables()) {
                String tableName = table.getName();
                String className = path + ".Udb" + tableName.substring(0, 1).toUpperCase() + tableName.substring(1);
                Class<?>  schemaClass = Class.forName(className);
                Method method = schemaClass.getDeclaredMethod("setTableIndex", TableIndex.class);
                method.setAccessible(true);
                method.invoke(null, table);
            }
        }

        if (clusterConfig != null) {
            cluster = new Cluster(clusterConfig, this);
        }
    }

    private UniversalDB(File storagePath, Schema schema, ClusterConfig clusterConfig) throws IOException {
        this.storagePath = storagePath;
        this.transactionStore = new TransactionStore(storagePath);
        this.schemaIndex = new SchemaIndex(new Schema(schema.getPojoNamespace()), storagePath);

        mapSchema(schema);
        if (clusterConfig != null) {
            cluster = new Cluster(clusterConfig, this);
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

    public synchronized void executeTransaction(ClusterTransaction transaction) throws IOException {
        TransactionRequest request = transaction.createRequest();
        if (cluster != null) {
            cluster.executeTransaction(request);
        } else {
            transactionStore.executeTransaction(request);
        }
    }

    public void synchronizeTransaction(ClusterTransaction transaction) throws IOException {
        transactionStore.synchronizeTransaction(transaction);
    }

    public SchemaIndex getSchemaIndex() {
        return schemaIndex;
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
    public long getCurrentTransactionId() {
        return transactionStore.getCurrentTransactionId();
    }

    @Override
    public long getLastTransactionId() {
        return transactionStore.getLastTransactionId();
    }

    @Override
    public long getTransactionCount() {
        return transactionStore.getTransactionCount();
    }

    @Override
    public Schema getSchema() {
        return transactionStore.getSchema();
    }

    @Override
    public void updateSchema(Schema schema) throws IOException {
        mapSchema(schema);
    }

    @Override
    public Iterator<byte[]> getTransactions(long startTransaction, long lastTransaction) {
        if (startTransaction == 0) {
            startTransaction = 8;
        }
        return transactionStore.getTransactions(startTransaction, lastTransaction);
    }

    @Override
    public void handleTransactionSynchronizationPacket(TransactionPacket packet) {
        try {
            ClusterTransaction transaction = new ClusterTransaction(packet, this);
            synchronizeTransaction(transaction);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public DataBaseMapper getDatabaseMapper() {
        return this;
    }

    @Override
    public TransactionIdProvider getTransactionIdProvider() {
        return transactionStore;
    }

    @Override
    public void executeTransactionRequest(TransactionRequest transactionRequest) throws IOException {
        transactionStore.executeTransaction(transactionRequest);
    }
}
