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

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.universaldb.index.FieldIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.file.FileIndex;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.file.store.DatabaseFileStore;
import org.teamapps.universaldb.index.file.value.FileValueType;
import org.teamapps.universaldb.index.file.value.StoreDescriptionFile;
import org.teamapps.universaldb.index.reference.value.MultiReferenceEditValue;
import org.teamapps.universaldb.index.reference.value.RecordReference;
import org.teamapps.universaldb.index.transaction.request.TransactionRequest;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecord;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordValue;
import org.teamapps.universaldb.model.FileFieldModel;

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

	public void addChangeValue(FieldIndex fieldIndex, Object value) {
		TransactionRequestRecordValue requestRecordValue = new TransactionRequestRecordValue(fieldIndex.getMappingId(), fieldIndex.getType(), value);
		changeMap.put(requestRecordValue.getColumnId(), requestRecordValue);
	}

	public void addRecordReference(RecordReference reference, Entity entity) {
		entityByReference.put(reference, entity);
	}

	public Map<RecordReference, Entity> getEntityByReference() {
		return entityByReference;
	}

	public TransactionRequestRecordValue getChangeValue2(FieldIndex index) {
		return changeMap.get(index.getMappingId());
	}

	public boolean isChanged(FieldIndex fieldIndex) {
		return changeMap.containsKey(fieldIndex.getMappingId());
	}

	public void removeChange(FieldIndex fieldIndex) {
		changeMap.remove(fieldIndex);
	}

	public TransactionRequestRecordValue getChangeValue(FieldIndex index) {
		return changeMap.get(index.getMappingId());
	}

	public void setTransactionRequestRecordValues(TransactionRequest transactionRequest, TransactionRequestRecord record, UniversalDB database) {
		List<AbstractUdbEntity> uncommittedEntityReferences = new ArrayList<>();
		List<TransactionRequestRecordValue> changeValues = new ArrayList<>(changeMap.values());
		for (TransactionRequestRecordValue recordValue : changeValues) {
			IndexType indexType = recordValue.getIndexType();
			Object value = recordValue.getValue();
			if (value == null) {
				record.addRecordValue(recordValue);
				continue;
			}
			switch (indexType) {
				case MULTI_REFERENCE -> {
					MultiReferenceEditValue editValue = (MultiReferenceEditValue) value;
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
					record.addRecordValue(recordValue);
				}
				case REFERENCE -> {
					RecordReference recordReference = (RecordReference) value;
					AbstractUdbEntity entity = getReferenceChange(recordValue.getColumnId());
					if (entity.getId() == 0) {
						uncommittedEntityReferences.add(entity);
					} else if (recordReference.getRecordId() == 0 && entity.getId() > 0) {
						recordReference.setRecordId(entity.getId());
					}
					record.addRecordValue(recordValue);
				}
				case FILE -> {
					FileValue fileValue = (FileValue) value;
					if (fileValue.getType() == FileValueType.UNCOMMITTED_FILE) {
						FileIndex fileIndex = (FileIndex) database.getColumnById(recordValue.getColumnId());
						FileFieldModel model = fileIndex.getFileFieldModel();
						DatabaseFileStore fileStore = database.getDatabaseIndex().getDatabaseFileStore();
						String key = fileStore.storeFile(fileValue.getAsFile(), fileValue.getHash(), fileValue.getSize());
						FileContentData contentData = model.isIndexContent() ? fileValue.getFileContentData(model.getMaxIndexContentLength()) : null;
						if (model.isIndexContent() && model.isDetectLanguage()) {
							fileValue.getDetectedLanguage();
						}
						StoreDescriptionFile storeDescriptionFile = new StoreDescriptionFile(null, fileValue.getFileName(), fileValue.getSize(), fileValue.getHash(), key, contentData);
						record.addRecordValue(fileIndex, storeDescriptionFile);
					} else {
						throw new RuntimeException("Error wrong file value type to save:" + fileValue.getType());
					}
				}
				default -> {
					record.addRecordValue(recordValue);
				}
			}
		}
		for (AbstractUdbEntity entity : uncommittedEntityReferences) {
			entity.saveRecord(transactionRequest, database);
		}
	}

	public void setReferenceChange(FieldIndex index, AbstractUdbEntity reference) {
		changedReferenceMap.put(index.getMappingId(), reference);
	}

	public AbstractUdbEntity getReferenceChange(int columnId) {
		return changedReferenceMap.get(columnId);
	}

}
