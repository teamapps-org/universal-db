package org.teamapps.universaldb.index.transaction.resolved;

import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.value.ResolvedMultiReferenceUpdate;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecord;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ResolvedTransactionRecord {

	private final ResolvedTransactionRecordType recordType;
	private final int tableId;
	private final int recordId;
	private final List<ResolvedTransactionRecordValue> recordValues = new ArrayList<>();

	public static ResolvedTransactionRecord createCyclicRecord(CyclicReferenceUpdate cyclicReferenceUpdate) {
		ResolvedTransactionRecordType recordType = cyclicReferenceUpdate.isRemoveReference() ? ResolvedTransactionRecordType.REMOVE_CYCLIC_REFERENCE : ResolvedTransactionRecordType.ADD_CYCLIC_REFERENCE;
		int tableId = cyclicReferenceUpdate.getReferenceIndex().getTable().getMappingId();
		int recordId = cyclicReferenceUpdate.getRecordId();
		ResolvedTransactionRecord record = new ResolvedTransactionRecord(recordType, tableId, recordId);

		int columnId = cyclicReferenceUpdate.getReferenceIndex().getMappingId();
		IndexType indexType = cyclicReferenceUpdate.getReferenceIndex().getType();
		Object value = cyclicReferenceUpdate.isSingleReference() ? cyclicReferenceUpdate.getReferencedRecordId() : ResolvedMultiReferenceUpdate.createAddRemoveReferences(cyclicReferenceUpdate);
		record.addRecordValue(new ResolvedTransactionRecordValue(columnId, indexType, value));

		return record;
	}

	public static ResolvedTransactionRecord createFromRequest(TransactionRequestRecord requestRecord, int recordId) {
		ResolvedTransactionRecordType recordType = ResolvedTransactionRecordType.getByRequestType(requestRecord.getRecordType());
		return new ResolvedTransactionRecord(recordType, requestRecord.getTableId(), recordId);
	}

	public ResolvedTransactionRecord(ResolvedTransactionRecordType recordType, int tableId, int recordId) {
		this.recordType = recordType;
		this.tableId = tableId;
		this.recordId = recordId;
	}

	public ResolvedTransactionRecord(DataInputStream dis) throws IOException {
		recordType = ResolvedTransactionRecordType.getById(dis.readByte());
		tableId = dis.readInt();
		recordId = dis.readInt();
		int count = dis.readInt();
		for (int i = 0; i < count; i++) {
			recordValues.add(new ResolvedTransactionRecordValue(dis));
		}
	}


	public void write(DataOutputStream dos) throws IOException {
		dos.writeByte(recordType.getId());
		dos.writeInt(tableId);
		dos.writeInt(recordId);
		dos.writeInt(recordValues.size());
		for (ResolvedTransactionRecordValue recordValue : recordValues) {
			recordValue.write(dos);
		}
	}

	public void addRecordValue(ResolvedTransactionRecordValue recordValue) {
		recordValues.add(recordValue);
	}

	public ResolvedTransactionRecordType getRecordType() {
		return recordType;
	}

	public List<ResolvedTransactionRecordValue> getRecordValues() {
		return recordValues;
	}

	public int getTableId() {
		return tableId;
	}

	public int getRecordId() {
		return recordId;
	}
}
