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
import org.teamapps.universaldb.index.reference.value.RecordReference;
import org.teamapps.universaldb.transaction.TransactionRecord;
import org.teamapps.universaldb.transaction.TransactionRecordValue;

import java.util.HashMap;
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

	public void setTransactionRecordValues(TransactionRecord transactionRecord) {
		changeMap.values().forEach(transactionRecord::addRecordValue);
	}

	public void setReferenceChange(ColumnIndex index, AbstractUdbEntity reference) {
		changedReferenceMap.put(index.getMappingId(), reference);
	}

	public AbstractUdbEntity getReferenceChange(ColumnIndex index) {
		return changedReferenceMap.get(index.getMappingId());
	}

}
