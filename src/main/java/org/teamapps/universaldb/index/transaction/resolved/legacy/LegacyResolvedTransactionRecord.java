/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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
package org.teamapps.universaldb.index.transaction.resolved.legacy;

import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.value.ResolvedMultiReferenceUpdate;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecord;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransactionRecordType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LegacyResolvedTransactionRecord {

	private final ResolvedTransactionRecordType recordType;
	private final int tableId;
	private final int recordId;
	private final List<LegacyResolvedTransactionRecordValue> recordValues = new ArrayList<>();

	public LegacyResolvedTransactionRecord(ResolvedTransactionRecordType recordType, int tableId, int recordId) {
		this.recordType = recordType;
		this.tableId = tableId;
		this.recordId = recordId;
	}

	public LegacyResolvedTransactionRecord(DataInputStream dis) throws IOException {
		recordType = ResolvedTransactionRecordType.getById(dis.readByte());
		tableId = dis.readInt();
		recordId = dis.readInt();
		int count = dis.readInt();
		for (int i = 0; i < count; i++) {
			recordValues.add(new LegacyResolvedTransactionRecordValue(dis));
		}
	}

	public static LegacyResolvedTransactionRecord createCyclicRecord(CyclicReferenceUpdate cyclicReferenceUpdate) {
		ResolvedTransactionRecordType recordType = cyclicReferenceUpdate.isRemoveReference() ? ResolvedTransactionRecordType.REMOVE_CYCLIC_REFERENCE : ResolvedTransactionRecordType.ADD_CYCLIC_REFERENCE;
		int tableId = cyclicReferenceUpdate.getReferenceIndex().getTable().getMappingId();
		int recordId = cyclicReferenceUpdate.getRecordId();
		LegacyResolvedTransactionRecord record = new LegacyResolvedTransactionRecord(recordType, tableId, recordId);

		int columnId = cyclicReferenceUpdate.getReferenceIndex().getMappingId();
		IndexType indexType = cyclicReferenceUpdate.getReferenceIndex().getType();
		Object value = cyclicReferenceUpdate.isSingleReference() ? cyclicReferenceUpdate.getReferencedRecordId() : ResolvedMultiReferenceUpdate.createAddRemoveReferences(cyclicReferenceUpdate);
		record.addRecordValue(new LegacyResolvedTransactionRecordValue(columnId, indexType, value));

		return record;
	}

	public static LegacyResolvedTransactionRecord createFromRequest(TransactionRequestRecord requestRecord, int recordId) {
		ResolvedTransactionRecordType recordType = ResolvedTransactionRecordType.getByRequestType(requestRecord.getRecordType());
		return new LegacyResolvedTransactionRecord(recordType, requestRecord.getTableId(), recordId);
	}

	public void write(DataOutputStream dos) throws IOException {
		dos.writeByte(recordType.getId());
		dos.writeInt(tableId);
		dos.writeInt(recordId);
		dos.writeInt(recordValues.size());
		for (LegacyResolvedTransactionRecordValue recordValue : recordValues) {
			recordValue.write(dos);
		}
	}

	public void addRecordValue(LegacyResolvedTransactionRecordValue recordValue) {
		recordValues.add(recordValue);
	}

	public ResolvedTransactionRecordType getRecordType() {
		return recordType;
	}

	public List<LegacyResolvedTransactionRecordValue> getRecordValues() {
		return recordValues;
	}

	public int getTableId() {
		return tableId;
	}

	public int getRecordId() {
		return recordId;
	}
}
