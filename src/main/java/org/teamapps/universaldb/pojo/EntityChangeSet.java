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
package org.teamapps.universaldb.pojo;

import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.reference.value.MultiReferenceEditValue;
import org.teamapps.universaldb.index.reference.value.RecordReference;
import org.teamapps.universaldb.index.transaction.request.TransactionRequest;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecord;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityChangeSet {

	private final Map<Integer, TransactionRequestRecordValue> changeMap;
	private final Map<Integer, AbstractUdbEntity> changedReferenceMap;
	private final Map<RecordReference, Entity> entityByReference;

	public EntityChangeSet() {
		changeMap = new HashMap<>();
		changedReferenceMap = new HashMap<>();
		entityByReference = new HashMap<>();
	}

	public void addChangeValue(ColumnIndex column, Object value) {
		TransactionRequestRecordValue requestRecordValue = new TransactionRequestRecordValue(column.getMappingId(), column.getType(), value);
		changeMap.put(requestRecordValue.getColumnId(), requestRecordValue);
	}

	public void addRecordReference(RecordReference reference, Entity entity) {
		entityByReference.put(reference, entity);
	}

	public Map<RecordReference, Entity> getEntityByReference() {
		return entityByReference;
	}

	public TransactionRequestRecordValue getChangeValue2(ColumnIndex index) {
		return changeMap.get(index.getMappingId());
	}

	public boolean isChanged(ColumnIndex columnIndex) {
		return changeMap.containsKey(columnIndex.getMappingId());
	}

	public void removeChange(ColumnIndex columnIndex) {
		changeMap.remove(columnIndex);
	}

	public TransactionRequestRecordValue getChangeValue(ColumnIndex index) {
		return changeMap.get(index.getMappingId());
	}

	public void setTransactionRequestRecordValues(TransactionRequest transactionRequest, TransactionRequestRecord transactionRequestRecord) {
		List<AbstractUdbEntity> uncommittedEntityReferences = new ArrayList<>();
		for (TransactionRequestRecordValue recordValue : changeMap.values()) {
			transactionRequestRecord.addRecordValue(recordValue);
			IndexType indexType = recordValue.getIndexType();
			if (recordValue.getValue() == null) {
				continue;
			}
			if (indexType == IndexType.MULTI_REFERENCE) {
				MultiReferenceEditValue editValue = (MultiReferenceEditValue) recordValue.getValue();
				for (RecordReference recordReference : editValue.getAddReferences()) {
					Entity entity = entityByReference.get(recordReference);
					if (entity.getId() == 0) {
						uncommittedEntityReferences.add((AbstractUdbEntity) entity);
					} else if (recordReference.getRecordId() == 0 && entity.getId() > 0) {
						recordReference.setRecordId(entity.getId());
					}
				}
				for (RecordReference recordReference : editValue.getSetReferences()) {
					Entity entity = entityByReference.get(recordReference);
					if (entity.getId() == 0) {
						uncommittedEntityReferences.add((AbstractUdbEntity) entity);
					} else if (recordReference.getRecordId() == 0 && entity.getId() > 0) {
						recordReference.setRecordId(entity.getId());
					}
				}
			} else if (indexType == IndexType.REFERENCE) {
				RecordReference recordReference = (RecordReference) recordValue.getValue();
				AbstractUdbEntity entity = getReferenceChange(recordValue.getColumnId());
				if (entity.getId() == 0) {
					uncommittedEntityReferences.add(entity);
				} else if (recordReference.getRecordId() == 0 && entity.getId() > 0) {
					recordReference.setRecordId(entity.getId());
				}
			}
		}
		for (AbstractUdbEntity entity : uncommittedEntityReferences) {
			entity.saveRecord(transactionRequest);
		}
	}

	public void setReferenceChange(ColumnIndex index, AbstractUdbEntity reference) {
		changedReferenceMap.put(index.getMappingId(), reference);
	}

	public AbstractUdbEntity getReferenceChange(int columnId) {
		return changedReferenceMap.get(columnId);
	}

}
