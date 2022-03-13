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
package org.teamapps.universaldb.transaction;

import org.teamapps.universaldb.index.DataBaseMapper;
import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
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

	public List<CyclicReferenceUpdate> persistChange(int id, Map<Integer, Integer> recordIdByCorrelationId) {
		if (column.getType() == IndexType.MULTI_REFERENCE) {
			MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) column;
			MultiReferenceEditValue editValue = (MultiReferenceEditValue) value;
			editValue.updateReferences(recordIdByCorrelationId);
			return multiReferenceIndex.setReferenceEditValue(id, editValue);
		} else if (column.getType() == IndexType.REFERENCE) {
			SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) column;
			if (value != null) {
				RecordReference recordReference = (RecordReference) value;
				recordReference.updateReference(recordIdByCorrelationId);
				return singleReferenceIndex.setReferenceValue(id, recordReference);
			} else {
				return singleReferenceIndex.setReferenceValue(id, null);
			}
		} else {
			column.setGenericValue(id, value);
		}
		return null;
	}

}
