/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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
package org.teamapps.universaldb.pojo;

import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.reference.value.MultiReferenceEditValue;
import org.teamapps.universaldb.index.reference.value.RecordReference;
import org.teamapps.universaldb.transaction.Transaction;
import org.teamapps.universaldb.transaction.TransactionRecord;
import org.teamapps.universaldb.transaction.TransactionRecordValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityChangeSet {

	private final Map<Integer, TransactionRecordValue> changeMap;
	private final Map<Integer, AbstractUdbEntity> changedReferenceMap;
	private final Map<RecordReference, Entity> entityByReference;

	public EntityChangeSet() {
		changeMap = new HashMap<>();
		changedReferenceMap = new HashMap<>();
		entityByReference = new HashMap<>();
	}

	public void addChangeValue(ColumnIndex column, Object value) {
		TransactionRecordValue recordValue = new TransactionRecordValue(column, value);
		changeMap.put(recordValue.getColumnMappingId(), recordValue);
	}

	public void addRecordReference(RecordReference reference, Entity entity) {
		entityByReference.put(reference, entity);
	}

	public Map<RecordReference, Entity> getEntityByReference() {
		return entityByReference;
	}

	public TransactionRecordValue getChangeValue(ColumnIndex index) {
		return changeMap.get(index.getMappingId());
	}

	public boolean isChanged(ColumnIndex columnIndex) {
		return changeMap.containsKey(columnIndex.getMappingId());
	}

	public void setTransactionRecordValues(Transaction transaction, TransactionRecord transactionRecord, boolean strictChangeVerification) {
		List<AbstractUdbEntity> uncommittedEntityReferences = new ArrayList<>();
		for (TransactionRecordValue recordValue : changeMap.values()) {
			transactionRecord.addRecordValue(recordValue);
			ColumnIndex column = recordValue.getColumn();
			if (recordValue.getValue() == null) {
				continue;
			}
			if (column.getType() == IndexType.MULTI_REFERENCE) {
				MultiReferenceEditValue editValue = (MultiReferenceEditValue) recordValue.getValue();
				for (RecordReference recordReference : editValue.getAddReferences()) {
					Entity entity = entityByReference.get(recordReference);
					if (entity.getId() == 0) {
						uncommittedEntityReferences.add((AbstractUdbEntity) entity);
					}
				}
				for (RecordReference recordReference : editValue.getSetReferences()) {
					Entity entity = entityByReference.get(recordReference);
					if (entity.getId() == 0) {
						uncommittedEntityReferences.add((AbstractUdbEntity) entity);
					}
				}
			} else if (column.getType() == IndexType.REFERENCE) {
				AbstractUdbEntity udbEntity = getReferenceChange(column);
				if (udbEntity.getId() == 0) {
					uncommittedEntityReferences.add(udbEntity);
				}
			}
		}
		for (AbstractUdbEntity entity : uncommittedEntityReferences) {
			TableIndex tableIndex = entity.getTableIndex();
			entity.save(transaction, tableIndex, strictChangeVerification);
		}
	}

	public void setReferenceChange(ColumnIndex index, AbstractUdbEntity reference) {
		changedReferenceMap.put(index.getMappingId(), reference);
	}

	public AbstractUdbEntity getReferenceChange(ColumnIndex index) {
		return changedReferenceMap.get(index.getMappingId());
	}

}
