/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;
import org.teamapps.universaldb.index.log.DefaultLogIndex;
import org.teamapps.universaldb.index.log.LogIndex;
import org.teamapps.universaldb.index.log.LogIterator;
import org.teamapps.universaldb.index.log.RotatingLogIndex;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransaction;
import org.teamapps.universaldb.index.transaction.schema.ModelUpdate;
import org.teamapps.universaldb.model.DatabaseModel;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
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
	private final LogIndex transactionLog;
	private final LogIndex modelsLog;
	private PrimitiveEntryAtomicStore databaseStats;
	private volatile boolean active = true;

	private DatabaseModel currentModel;
	private ModelUpdate currentModelUpdate;

	public TransactionIndex(File path, boolean skipIndexCheck) {
		this.path = path;
		this.transactionLog = new RotatingLogIndex(this.path, "transactions");
		this.modelsLog = new DefaultLogIndex(this.path, "models");
		this.databaseStats = new PrimitiveEntryAtomicStore(this.path, "db-stats");
		logger.info("Open transaction index on: {}", path.getAbsolutePath());
		init();
		if (!skipIndexCheck) {
			checkIndex();
		}
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

	private void init() {
		if (getNodeId() == 0) {
			databaseStats.setLong(NODE_ID, createId());
		}
		if (getSystemFirstStart() == 0) {
			databaseStats.setLong(FIRST_SYSTEM_START, System.currentTimeMillis());
		}
		databaseStats.setLong(LAST_SYSTEM_START, System.currentTimeMillis());
		currentModelUpdate = getModelUpdates().stream().reduce((first, second) -> second).orElse(null);
		currentModel = currentModelUpdate == null ? null : currentModelUpdate.getMergedModel();
		logger.info(UniversalDB.SKIP_DB_LOGGING, "STARTED TRANSACTION INDEX: node-id: {}, last-transaction-id: {}, last-transaction-store-id: {}, transaction-count: {}, last-request-id: {}, schema-updates: {}", getNodeIdAsString(), getLastTransactionId(), getLastTransactionStoreId(), getTransactionCount(), getLastTransactionRequestId(), getModelUpdates().size());
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				active = false;
				logger.info(UniversalDB.SKIP_DB_LOGGING, "SHUTTING DOWN TRANSACTION INDEX: node-id: {}, last-transaction-id: {}, last-transaction-store-id: {}, transaction-count: {}, last-request-id: {}", getNodeIdAsString(), getLastTransactionId(), getLastTransactionStoreId(), getTransactionCount(), getLastTransactionRequestId());
				databaseStats.setLong(TIMESTAMP_SHUTDOWN, System.currentTimeMillis());
				transactionLog.close();
				modelsLog.close();
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
		logger.info(UniversalDB.SKIP_DB_LOGGING, "Checking transaction index...");
		while (logIterator.hasNext()) {
			ResolvedTransaction transaction = ResolvedTransaction.createResolvedTransaction(logIterator.next());
			if (expectedTransactionId != transaction.getTransactionId()) {
				logger.error(UniversalDB.SKIP_DB_LOGGING, "Wrong transaction id: {}, expected: {}", transaction.getTransactionId(), expectedTransactionId);
				ok = false;
			}
			expectedTransactionId = transaction.getTransactionId() + 1;
		}
		logger.info(UniversalDB.SKIP_DB_LOGGING, "Transaction index check result: {}", ok);
		logIterator.closeSave();
		if (!ok) {
			throw new RuntimeException("Error in transaction log!");
		}
		if (getLastTransactionId() != (expectedTransactionId - 1)) {
			logger.error("Wrong transaction id in stats file, expected: {}, actual: {}, probably system was not shut down properly", (expectedTransactionId - 1), getLastTransactionId());
			if (new File(path, "transaction-log.repair").exists()) {
				logger.warn("Fixing transaction stats file...");
				databaseStats.setLong(LAST_TRANSACTION_ID, expectedTransactionId - 1);
				new File(path, "transaction-log.repair").delete();
				logger.warn("Transaction stats file fixed, repair-file removed");
			} else {
				logger.warn("To fix the transaction id: create a file 'transaction-log.repair' and put it into the directory: {} and restart", path.getPath());
				throw new RuntimeException("Error last transaction id not matching transaction log!");
			}
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

	public synchronized boolean isValidModel(DatabaseModel model) {
		return (currentModel == null && model.isValid()) || currentModel.isCompatible(model);
	}

	public synchronized boolean isModelUpdate(DatabaseModel model) {
//		long maxBuildTime = getModelUpdates().stream().mapToLong(update -> update.getDatabaseModel().getPojoBuildTime()).max().orElse(0);
//		return model.getPojoBuildTime() > maxBuildTime;
		if (currentModelUpdate != null && currentModelUpdate.getDatabaseModel().isSameModel(model)) {
			return false;
		}
		return getModelUpdates()
				.stream()
				.noneMatch(update -> update.getDatabaseModel().getPojoBuildTime() == model.getPojoBuildTime());
	}

	public synchronized void writeModelUpdate(ModelUpdate modelUpdate) throws IOException {
		currentModelUpdate = modelUpdate;
		DatabaseModel model = modelUpdate.getDatabaseModel();
		if (currentModel == null) {
			currentModel = model;
			currentModel.initialize(); //todo copy!!!
		} else {
			currentModel.mergeModel(model);
		}
		modelUpdate.setMergedModel(currentModel);
		modelsLog.writeLog(modelUpdate.getBytes());
		logger.info(UniversalDB.SKIP_DB_LOGGING, "Updating schema");
	}

	public synchronized DatabaseModel getCurrentModel() {
		return currentModel;
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
		databaseStats.flush();
	}

	public synchronized List<ModelUpdate> getModelUpdates() {
		if (modelsLog.isEmpty()) {
			return Collections.emptyList();
		}
		return modelsLog.readAllLogs()
				.stream()
				.map(ModelUpdate::new)
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


}
