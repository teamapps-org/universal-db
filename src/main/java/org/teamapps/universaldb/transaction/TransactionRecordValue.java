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
package org.teamapps.universaldb.transaction;

import org.teamapps.universaldb.index.DataBaseMapper;
import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.reference.value.MultiReferenceEditValue;
import org.teamapps.universaldb.index.reference.value.RecordReference;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TransactionRecordValue {

	private final ColumnIndex column;
	private final Object value;


	public TransactionRecordValue(ColumnIndex column, Object value) {
		this.column = column;
		this.value = value;
	}

	public TransactionRecordValue(DataInputStream dataInputStream, DataBaseMapper dataBaseMapper) throws IOException {
		this.column = dataBaseMapper.getColumnById(dataInputStream.readInt());
		int type = dataInputStream.readByte();
		if (type == 0) {
			value = null;
		} else {
			value = column.readTransactionValue(dataInputStream);
		}
	}

	public ColumnIndex getColumn() {
		return column;
	}

	public Object getValue() {
		return value;
	}

	public int getColumnMappingId() {
		return column.getMappingId();
	}

	public void writeTransactionValue(DataOutputStream dataOutputStream) throws IOException {
		if (value == null) {
			dataOutputStream.writeInt(column.getMappingId());
			dataOutputStream.writeByte(0);
		} else {
			column.writeTransactionValue(value, dataOutputStream);
		}
	}

	public void persistChange(int id, Map<Integer, Integer> recordIdByCorrelationId) {
		if (column.getType() == IndexType.MULTI_REFERENCE) {
			MultiReferenceEditValue editValue = (MultiReferenceEditValue) value;
			updateReferences(recordIdByCorrelationId, editValue.getAddReferences());
			updateReferences(recordIdByCorrelationId, editValue.getRemoveReferences());
			updateReferences(recordIdByCorrelationId, editValue.getSetReferences());
			column.setGenericValue(id, editValue);
		} else if (column.getType() == IndexType.REFERENCE) {
			if (value != null) {
				RecordReference recordReference = (RecordReference) value;
				if (recordReference.getRecordId() == 0) {
					int recordId = recordIdByCorrelationId.get(recordReference.getCorrelationId());
					recordReference.setRecordId(recordId);
				}
				column.setGenericValue(id, recordReference);
			} else {
				column.setGenericValue(id, null);
			}
		} else {
			column.setGenericValue(id, value);
		}
	}

	public void updateReferences(Map<Integer, Integer> recordIdByCorrelationId, List<RecordReference> references) {
		references.forEach(reference -> {
			if (reference.getRecordId() == 0) {
				int recordId = recordIdByCorrelationId.get(reference.getCorrelationId());
				reference.setRecordId(recordId);
			}
		});
	}

}
