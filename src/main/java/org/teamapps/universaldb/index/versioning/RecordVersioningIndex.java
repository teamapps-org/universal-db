package org.teamapps.universaldb.index.versioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.buffer.PrimitiveEntryAtomicStore;
import org.teamapps.universaldb.index.nonmap.SequentialStore;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.transaction.TransactionRecord;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

public class RecordVersioningIndex {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final File dataPath;
	private final TableIndex table;

	private PrimitiveEntryAtomicStore atomicStore;
	private SequentialStore sequentialStore;

	public RecordVersioningIndex(TableIndex table) {
		this.table = table;
		this.dataPath = table.getDataPath();
		this.atomicStore = new PrimitiveEntryAtomicStore(dataPath, "versioning-pos.vdx");
		try {
			this.sequentialStore = new SequentialStore(dataPath, "versioning-seq.vdx");
		} catch (IOException e) {
			throw new RuntimeException("Error creating versioning index", e);
		}
	}

	public void checkVersionIndex() {
		try {
			if (sequentialStore.getPosition() == 4 && table.getCount() > 0) {
				long time = System.currentTimeMillis();
				logger.info("Empty version index, write index for: {}", table.getFQN());
				writeInitialValues();
				logger.info("Finished writing version index for: {}, time: {}", table.getName(), (System.currentTimeMillis() - time));
			}
		} catch (IOException e) {
			throw new RuntimeException("Error writing initial versioning index", e);
		}
	}

	public List<RecordUpdate> readRecordUpdates(int recordId) throws IOException {
		long storePos = atomicStore.getLong(recordId);
		if (storePos == 0) {
			return Collections.emptyList();
		} else {
			List<RecordUpdate> recordUpdates = new ArrayList<>();
			byte[] bytes = sequentialStore.read(storePos);
			RecordUpdate recordUpdate = new RecordUpdate(bytes);
			recordUpdates.add(recordUpdate);
			while (recordUpdate.getPreviousPosition() > 0) {
				bytes = sequentialStore.read(recordUpdate.getPreviousPosition());
				recordUpdate = new RecordUpdate(bytes);
				recordUpdates.add(recordUpdate);
			}
			Collections.reverse(recordUpdates);
			return recordUpdates;
		}
	}

	public void writeUpdate(TransactionRecord transactionRecord, int recordId, long timestamp, int userId, long transactionId, Map<Integer, Integer> recordIdByCorrelationId) {
		try {
			long previousPosition = atomicStore.getLong(recordId);
			byte[] updateEntry = RecordUpdate.createUpdate(previousPosition, transactionRecord, recordId, userId, timestamp, transactionId, recordIdByCorrelationId);
			long newPosition = sequentialStore.writeCommitted(updateEntry);
			atomicStore.setLong(recordId, newPosition);
		} catch (IOException e) {
			throw new RuntimeException("Error creating new record update", e);
		}
	}

	public void writeCyclicReference(CyclicReferenceUpdate cyclicReferenceUpdate, int userId, long timestamp, long transactionId) {
		try {
			int recordId = cyclicReferenceUpdate.getRecordId();
			if (recordId == 0) {
				throw new RuntimeException("Error creating cyclic reference update, missing record id");
			}
			long previousPosition = atomicStore.getLong(recordId);
			byte[] updateEntry = RecordUpdate.writeCyclicReference(previousPosition, cyclicReferenceUpdate, userId, timestamp, transactionId);
			long newPosition = sequentialStore.writeCommitted(updateEntry);
			atomicStore.setLong(recordId, newPosition);
		} catch (IOException e) {
			throw new RuntimeException("Error creating cyclic reference update", e);
		}
	}

	public void writeInitialValues() throws IOException {
		BitSet records = table.getRecords();
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			writeInitialRecord(id);
		}
		if (table.isKeepDeletedRecords()) {
			records = table.getDeletedRecords();
			for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
				long storePosition = writeInitialRecord(id);
				writeInitialDeletion(id, storePosition);
			}
		}
		sequentialStore.flush();
	}

	private long writeInitialRecord(int recordId) throws IOException {
		byte[] updateEntry = RecordUpdate.createInitialUpdate(table, recordId);
		long storePosition = sequentialStore.write(updateEntry);
		atomicStore.setLong(recordId, storePosition);
		return storePosition;
	}

	private void writeInitialDeletion(int recordId, long storePosition) throws IOException {
		byte[] updateEntry = RecordUpdate.createInitialDeletionUpdate(storePosition, recordId, table);
		long newPostion = sequentialStore.write(updateEntry);
		atomicStore.setLong(recordId, newPostion);
	}

}
