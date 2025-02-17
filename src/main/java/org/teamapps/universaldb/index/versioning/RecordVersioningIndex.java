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
package org.teamapps.universaldb.index.versioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;
import org.teamapps.universaldb.index.log.DefaultLogIndex;
import org.teamapps.universaldb.index.log.LogIndex;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransaction;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecord;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

public class RecordVersioningIndex {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final File dataPath;
	private final TableIndex table;
	private PrimitiveEntryAtomicStore positionsIndex;
	private LogIndex logIndex;

	public RecordVersioningIndex(TableIndex table) {
		this.table = table;
		this.dataPath = table.getDataPath();
		this.positionsIndex = new PrimitiveEntryAtomicStore(dataPath, "versioning-pos");
		this.logIndex = new DefaultLogIndex(dataPath, "versioning-log.vdx");
	}

	public boolean isEmpty() {
		return logIndex.isEmpty();
	}

	public void checkVersionIndex(UniversalDB universalDB) {
		try {
			if (logIndex.isEmpty() && table.getCount() > 0) {
				long time = System.currentTimeMillis();
				logger.info("Empty version index, write index for: {}", table.getFQN());
				universalDB.createInitialTableTransactions(table);
				logger.info("Finished writing version index for: {}, time: {}", table.getName(), (System.currentTimeMillis() - time));
			}
		} catch (Exception e) {
			throw new RuntimeException("Error writing initial versioning index", e);
		}
	}

	public List<RecordUpdate> readRecordUpdates(int recordId) throws IOException {
		long storePos = positionsIndex.getLong(recordId);
		if (storePos == 0) {
			return Collections.emptyList();
		} else {
			List<RecordUpdate> recordUpdates = new ArrayList<>();
			byte[] bytes = logIndex.readLog(storePos);
			RecordUpdate recordUpdate = new RecordUpdate(bytes);
			recordUpdates.add(recordUpdate);
			while (recordUpdate.getPreviousPosition() > 0) {
				bytes = logIndex.readLog(recordUpdate.getPreviousPosition());
				recordUpdate = new RecordUpdate(bytes);
				recordUpdates.add(recordUpdate);
			}
			Collections.reverse(recordUpdates);
			return recordUpdates;
		}
	}

	public void writeRecordUpdate(ResolvedTransaction transaction, ResolvedTransactionRecord record) {
		writeRecordUpdate(RecordUpdate.createUpdate(transaction, record));
	}

	public void writeRecordUpdate(RecordUpdate recordUpdate) {
		try {
			long previousPosition = positionsIndex.getLong(recordUpdate.getRecordId());
			recordUpdate.setPreviousPosition(previousPosition);
			byte[] updateEntry = recordUpdate.getBytes();
			long newPosition = logIndex.writeLog(updateEntry);
			positionsIndex.setLong(recordUpdate.getRecordId(), newPosition);
		} catch (IOException e) {
			throw new RuntimeException("Error creating new record update", e);
		}
	}

	public void close() {
		try {
			positionsIndex.close();
			logIndex.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void drop() {
		positionsIndex.drop();
		logIndex.drop();
	}

}
