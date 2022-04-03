package org.teamapps.universaldb.index.versioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.buffer.PrimitiveEntryAtomicStore;
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

//	public void checkVersionIndex() {
//		try {
//			if (logIndex.isEmpty() && table.getCount() > 0) {
//				long time = System.currentTimeMillis();
//				logger.info("Empty version index, write index for: {}", table.getFQN());
//				writeInitialValues();
//				logger.info("Finished writing version index for: {}, time: {}", table.getName(), (System.currentTimeMillis() - time));
//			}
//		} catch (IOException e) {
//			throw new RuntimeException("Error writing initial versioning index", e);
//		}
//	}

	public List<RecordUpdate2> readRecordUpdates(int recordId) throws IOException {
		long storePos = positionsIndex.getLong(recordId);
		if (storePos == 0) {
			return Collections.emptyList();
		} else {
			List<RecordUpdate2> recordUpdates = new ArrayList<>();
			byte[] bytes = logIndex.readLog(storePos);
			RecordUpdate2 recordUpdate = new RecordUpdate2(bytes);
			recordUpdates.add(recordUpdate);
			while (recordUpdate.getPreviousPosition() > 0) {
				bytes = logIndex.readLog(recordUpdate.getPreviousPosition());
				recordUpdate = new RecordUpdate2(bytes);
				recordUpdates.add(recordUpdate);
			}
			Collections.reverse(recordUpdates);
			return recordUpdates;
		}
	}

	public void writeRecordUpdate(ResolvedTransaction transaction, ResolvedTransactionRecord record) {
		writeRecordUpdate(RecordUpdate2.createUpdate(transaction, record));
	}

	public void writeRecordUpdate(RecordUpdate2 recordUpdate) {
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

//	public List<RecordUpdate> readRecordUpdates(int recordId) throws IOException {
//		long storePos = positionsIndex.getLong(recordId);
//		if (storePos == 0) {
//			return Collections.emptyList();
//		} else {
//			List<RecordUpdate> recordUpdates = new ArrayList<>();
//			byte[] bytes = logIndex.readLog(storePos);
//			RecordUpdate recordUpdate = new RecordUpdate(bytes);
//			recordUpdates.add(recordUpdate);
//			while (recordUpdate.getPreviousPosition() > 0) {
//				bytes = logIndex.readLog(recordUpdate.getPreviousPosition());
//				recordUpdate = new RecordUpdate(bytes);
//				recordUpdates.add(recordUpdate);
//			}
//			Collections.reverse(recordUpdates);
//			return recordUpdates;
//		}
//	}

//	public void writeUpdate(TransactionRecord transactionRecord, int recordId, long timestamp, int userId, long transactionId, Map<Integer, Integer> recordIdByCorrelationId) {
//		try {
//			long previousPosition = positionsIndex.getLong(recordId);
//			byte[] updateEntry = RecordUpdate.createUpdate(previousPosition, transactionRecord, recordId, userId, timestamp, transactionId, recordIdByCorrelationId);
//			long newPosition = logIndex.writeLog(updateEntry);
//			positionsIndex.setLong(recordId, newPosition);
//		} catch (IOException e) {
//			throw new RuntimeException("Error creating new record update", e);
//		}
//	}

//	public void writeCyclicReference(CyclicReferenceUpdate cyclicReferenceUpdate, int userId, long timestamp, long transactionId) {
//		try {
//			int recordId = cyclicReferenceUpdate.getRecordId();
//			if (recordId == 0) {
//				throw new RuntimeException("Error creating cyclic reference update, missing record id");
//			}
//			long previousPosition = positionsIndex.getLong(recordId);
//			byte[] updateEntry = RecordUpdate.writeCyclicReference(previousPosition, cyclicReferenceUpdate, userId, timestamp, transactionId);
//			long newPosition = logIndex.writeLog(updateEntry);
//			positionsIndex.setLong(recordId, newPosition);
//		} catch (IOException e) {
//			throw new RuntimeException("Error creating cyclic reference update", e);
//		}
//	}

//	public void writeInitialValues() throws IOException {
//		BitSet records = table.getRecords();
//		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
//			writeInitialRecord(id);
//		}
//		if (table.isKeepDeletedRecords()) {
//			records = table.getDeletedRecords();
//			for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
//				long storePosition = writeInitialRecord(id);
//				writeInitialDeletion(id, storePosition);
//			}
//		}
//		logIndex.flush();
//	}

//	private long writeInitialRecord(int recordId) throws IOException {
//		byte[] updateEntry = RecordUpdate.createInitialUpdate(table, recordId);
//		long storePosition = logIndex.writeLog(updateEntry, false);
//		positionsIndex.setLong(recordId, storePosition);
//		return storePosition;
//	}
//
//	private void writeInitialDeletion(int recordId, long storePosition) throws IOException {
//		byte[] updateEntry = RecordUpdate.createInitialDeletionUpdate(storePosition, recordId, table);
//		long newPostion = logIndex.writeLog(updateEntry, false);
//		positionsIndex.setLong(recordId, newPostion);
//	}

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
