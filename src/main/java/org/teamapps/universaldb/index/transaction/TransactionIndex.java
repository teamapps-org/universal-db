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
package org.teamapps.universaldb.index.transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.index.buffer.PrimitiveEntryAtomicStore;
import org.teamapps.universaldb.index.log.DefaultLogIndex;
import org.teamapps.universaldb.index.log.LogIndex;
import org.teamapps.universaldb.index.log.LogIterator;
import org.teamapps.universaldb.index.log.RotatingLogIndex;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransaction;
import org.teamapps.universaldb.index.transaction.schema.SchemaUpdate;
import org.teamapps.universaldb.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TransactionIndex {
	private static final int FIRST_SYSTEM_START = 1;
	private static final int LAST_SYSTEM_START = 2;
	private static final int TIMESTAMP_SHUTDOWN = 3;
	private static final int LAST_TRANSACTION_ID = 4;
	private static final int LAST_TRANSACTION_STORE_ID = 5;
	private static final int LAST_TRANSACTION_REQUEST_ID = 6;
	private static final int TRANSACTIONS_COUNT = 7;
	private static final int NODE_ID = 8;

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final File path;
	private LogIndex transactionLog;
	private LogIndex schemaLog;
	private PrimitiveEntryAtomicStore databaseStats;
	private volatile boolean active = true;

	private Schema currentSchema;

	public TransactionIndex(File basePath) {
		this.path = new File(basePath, "transactions");
		this.path.mkdir();
		this.transactionLog = new RotatingLogIndex(this.path, "transactions");
		this.schemaLog = new DefaultLogIndex(this.path, "schemas");
		this.databaseStats = new PrimitiveEntryAtomicStore(this.path, "db-stats");
		init();
		checkIndex();
	}

	private void init() {
		if (getNodeId() == 0) {
			databaseStats.setLong(NODE_ID, createId());
		}
		if (getSystemFirstStart() == 0) {
			databaseStats.setLong(FIRST_SYSTEM_START, System.currentTimeMillis());
		}
		databaseStats.setLong(LAST_SYSTEM_START, System.currentTimeMillis());
		if (!schemaLog.isEmpty()) {
			List<SchemaUpdate> schemaUpdates = getSchemaUpdates();
			currentSchema = getSchemaUpdates().get(schemaUpdates.size() - 1).getSchema();
		}
		logger.info("STARTED TRANSACTION INDEX: node-id: {}, last-transaction-id: {}, last-transaction-store-id: {}, transaction-count: {}, last-request-id: {}, schema-updates: {}", getNodeIdAsString(), getLastTransactionId(), getLastTransactionStoreId(), getTransactionCount(), getLastTransactionRequestId(), getSchemaUpdates().size());
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				active = false;
				logger.info("SHUTTING DOWN TRANSACTION INDEX: node-id: {}, last-transaction-id: {}, last-transaction-store-id: {}, transaction-count: {}, last-request-id: {}", getNodeIdAsString(), getLastTransactionId(), getLastTransactionStoreId(), getTransactionCount(), getLastTransactionRequestId());
				databaseStats.setLong(TIMESTAMP_SHUTDOWN, System.currentTimeMillis());
				transactionLog.close();
				schemaLog.close();
				databaseStats.flush();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}));
	}

	private boolean checkIndex() {
		LogIterator logIterator = transactionLog.readLogs();
		long expectedTransactionId = 1;
		boolean ok = true;
		logger.info("Checking transaction index...");
		while (logIterator.hasNext()) {
			ResolvedTransaction transaction = ResolvedTransaction.createResolvedTransaction(logIterator.next());
			if (expectedTransactionId != transaction.getTransactionId()) {
				logger.error("Wrong transaction id: {}, expected: {}", transaction.getTransactionId(), expectedTransactionId);
				ok = false;
			}
			expectedTransactionId = transaction.getTransactionId() + 1;
		}
		logger.info("Transaction index check result: {}", ok);
		if (!ok) {
			throw new RuntimeException("Error in transaction log!");
		}
		return ok;
	}

	public boolean isEmpty() {
		return transactionLog.isEmpty();
	}

	public synchronized long createTransactionRequestId() {
		long requestId = getLastTransactionRequestId() + 1;
		databaseStats.setLong(LAST_TRANSACTION_REQUEST_ID, requestId);
		return requestId;
	}

	public long getSystemFirstStart() {
		return databaseStats.getLong(FIRST_SYSTEM_START);
	}

	public long getSystemLastStart() {
		return databaseStats.getLong(LAST_SYSTEM_START);
	}

	public long getLastTransactionId() {
		return databaseStats.getLong(LAST_TRANSACTION_ID);
	}

	public long getLastTransactionStoreId() {
		return databaseStats.getLong(LAST_TRANSACTION_STORE_ID);
	}

	public long getTransactionCount() {
		return databaseStats.getLong(TRANSACTIONS_COUNT);
	}

	public long getLastTransactionRequestId() {
		return databaseStats.getLong(LAST_TRANSACTION_REQUEST_ID);
	}

	public long getNodeId() {
		return databaseStats.getLong(NODE_ID);
	}

	public String getNodeIdAsString() {
		return Long.toHexString(getNodeId()).toUpperCase();
	}

	public ResolvedTransaction getLastTransaction() {
		if (transactionLog.isEmpty()) {
			return null;
		} else {
			byte[] bytes = transactionLog.readLog(getLastTransactionStoreId());
			return ResolvedTransaction.createResolvedTransaction(bytes);
		}
	}

	public synchronized boolean isValidSchema(Schema schema) {
		if (currentSchema != null) {
			if (!currentSchema.isCompatibleWith(schema)) {
				return false;
			}
			Schema schemaCopy = Schema.parse(currentSchema.getSchemaDefinition());
			schemaCopy.merge(schema);
			schemaCopy.mapSchema();
			if (!schemaCopy.checkModel()) {
				return false;
			}
		}
		return true;
	}

	public synchronized boolean isSchemaUpdate(Schema schema) {
		if (currentSchema == null) {
			return true;
		}
		Schema schemaCopy = Schema.parse(currentSchema.getSchemaDefinition());
		schemaCopy.merge(schema);
		schemaCopy.mapSchema();
		return !currentSchema.getSchemaDefinition().equals(schemaCopy.getSchemaDefinition());
	}

	public synchronized void writeSchemaUpdate(SchemaUpdate schemaUpdate) throws IOException {
		schemaLog.writeLog(schemaUpdate.getBytes());
		currentSchema = schemaUpdate.getSchema();
		logger.info("Updating schema");
	}

	public synchronized Schema getCurrentSchema() {
		return currentSchema;
	}

	public synchronized void writeTransaction(ResolvedTransaction transaction) throws Exception {
		if (!active) {
			throw new RuntimeException("Error transaction index already shut down");
		}
		if (transaction.getTransactionId() != getLastTransactionId() + 1) {
			throw new RuntimeException(String.format("Error wrong transaction id: %s, last transaction id: %s", transaction.getTransactionId(), getLastTransactionId()));
		}
		transactionLog.writeLog(transaction.getBytes());
		databaseStats.setLong(LAST_TRANSACTION_ID, transaction.getTransactionId());
		databaseStats.setLong(LAST_TRANSACTION_STORE_ID, transactionLog.getPosition());
		databaseStats.setLong(TRANSACTIONS_COUNT, getTransactionCount() + 1);
	}

	public synchronized List<SchemaUpdate> getSchemaUpdates() {
		if (schemaLog.isEmpty()) {
			return Collections.emptyList();
		}
		return schemaLog.readAllLogs()
				.stream()
				.map(SchemaUpdate::new)
				.collect(Collectors.toList());
	}

	public Stream<ResolvedTransaction> getTransactions(long lastTransactionId) {
		LogIterator logIterator = transactionLog.readLogs();
		Stream<byte[]> stream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(logIterator, Spliterator.ORDERED), false);
		//todo close log iterator?
		return stream
				.map(ResolvedTransaction::createResolvedTransaction)
				.filter(transaction -> transaction.getTransactionId() > lastTransactionId);
	}

	public LogIterator getLogIterator() {
		return transactionLog.readLogs();
	}

	private static long createId() {
		SecureRandom secureRandom = new SecureRandom();
		while (true) {
			long id = Math.abs(secureRandom.nextLong());
			if (Long.toHexString(id).length() == 16) {
				return id;
			}
		}
	}


}
