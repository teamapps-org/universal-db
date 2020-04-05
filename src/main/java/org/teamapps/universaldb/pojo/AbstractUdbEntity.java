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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.SortEntry;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.reference.value.*;
import org.teamapps.universaldb.record.EntityBuilder;
import org.teamapps.universaldb.transaction.Transaction;
import org.teamapps.universaldb.transaction.TransactionRecord;
import org.teamapps.universaldb.transaction.TransactionRecordValue;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class AbstractUdbEntity<ENTITY extends Entity> implements Entity<ENTITY> {

	private static final Logger log = LoggerFactory.getLogger(AbstractUdbEntity.class);
	private static final AtomicInteger correlationIdGenerator = new AtomicInteger();
	private static final int MAX_CORRELATION_ID = 2_000_000_000;

	private final TableIndex tableIndex;
	private int id;
	private boolean createEntity;
	private int correlationId;
	private EntityChangeSet entityChangeSet;
	private Transaction transaction;

	public static <ENTITY extends Entity> List<ENTITY> sort(TableIndex table, List<ENTITY> list, String sortFieldName, boolean ascending, String ... path) {
		return sort(table, list, sortFieldName, ascending, null, path);
	}

	public static <ENTITY extends Entity> List<ENTITY> sort(TableIndex table, List<ENTITY> list, String sortFieldName, boolean ascending, Locale locale, String ... path) {
		SingleReferenceIndex[] referencePath = getReferenceIndices(table, path);
		ColumnIndex column = getSortColumn(table, sortFieldName, referencePath);
		List<SortEntry<ENTITY>> sortEntries = SortEntry.createSortEntries(list, referencePath);
		sortEntries = column.sortRecords(sortEntries, ascending, locale);
		return sortEntries.stream().map(SortEntry::getEntity).collect(Collectors.toList());
	}

	public static <ENTITY extends Entity> List<ENTITY> sort(TableIndex table, EntityBuilder<ENTITY> builder, BitSet recordIds, String sortFieldName, boolean ascending, String ... path) {
		return sort(table, builder, recordIds, sortFieldName, ascending, null, path);
	}

	public static <ENTITY extends Entity> List<ENTITY> sort(TableIndex table, EntityBuilder<ENTITY> builder, BitSet recordIds, String sortFieldName, boolean ascending, Locale locale, String ... path) {
		SingleReferenceIndex[] referencePath = getReferenceIndices(table, path);
		ColumnIndex column = getSortColumn(table, sortFieldName, referencePath);
		List<SortEntry> sortEntries = SortEntry.createSortEntries(recordIds, referencePath);
		sortEntries = column.sortRecords(sortEntries, ascending, locale);
		List<ENTITY> list = new ArrayList<>();
		for (SortEntry entry : sortEntries) {
			list.add(builder.build(entry.getId()));
		}
		return list;
	}

	private static SingleReferenceIndex[] getReferenceIndices(TableIndex table, String[] path) {
		SingleReferenceIndex[] referencePath = null;
		if (path != null && path.length > 0) {
			referencePath = new SingleReferenceIndex[path.length];
			TableIndex pathTable = table;
			for (int i = 0; i < path.length; i++) {
				referencePath[i] = (SingleReferenceIndex) pathTable.getColumnIndex(path[i]);
				pathTable = referencePath[i].getReferencedTable();
			}
		}
		return referencePath;
	}

	private static ColumnIndex getSortColumn(TableIndex table, String sortFieldName, SingleReferenceIndex[] referencePath) {
		ColumnIndex column;
		if (referencePath != null && referencePath.length > 0) {
			column = referencePath[referencePath.length - 1].getReferencedTable().getColumnIndex(sortFieldName);
		} else {
			column = table.getColumnIndex(sortFieldName);
		}
		return column;
	}

	public AbstractUdbEntity(TableIndex tableIndex) {
		this.tableIndex = tableIndex;
		createEntity = true;
		createCorrelationId();
	}

	public AbstractUdbEntity(TableIndex tableIndex, int id, boolean createEntity) {
		this.tableIndex = tableIndex;
		this.id = id;
		this.createEntity = createEntity;
		if (createEntity) {
			createCorrelationId();
		}
	}

	private void createCorrelationId() {
		if (correlationId > 0) {
			return;
		}
		correlationId = correlationIdGenerator.incrementAndGet();
		if (correlationId == MAX_CORRELATION_ID) {
			correlationIdGenerator.set(0);
		}
	}

	@Override
	public int getId() {
		if (id == 0 && transaction != null) {
			id = transaction.getResolvedRecordIdByCorrelationId(correlationId);
		}
		return id;
	}

	protected int getCorrelationId() {
		return correlationId;
	}

	protected void setChangeValue(ColumnIndex index, Object value, TableIndex tableIndex) {
		checkChangeSet();
		entityChangeSet.addChangeValue(index, value);
	}

	protected void setSingleReferenceValue(ColumnIndex index, Entity reference, TableIndex tableIndex) {
		AbstractUdbEntity entity = (AbstractUdbEntity) reference;
		RecordReference recordReference = null;
		if (entity != null) {
			recordReference = new RecordReference(entity.getId(), entity.getCorrelationId());
		}
		checkChangeSet();
		entityChangeSet.addChangeValue(index, recordReference);
		entityChangeSet.setReferenceChange(index, entity);
	}

	protected <OTHER_ENTITY extends Entity> List<OTHER_ENTITY> createEntityList(ColumnIndex index, EntityBuilder<OTHER_ENTITY> entityBuilder) {
		TransactionRecordValue changeValue = getChangeValue(index);
		MultiReferenceEditValue editValue = (MultiReferenceEditValue) changeValue.getValue();
		MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) index;
		PrimitiveIterator.OfInt references = multiReferenceIndex.getReferences(getId());
		return createEntityList(editValue, references, entityBuilder);
	}

	protected <OTHER_ENTITY extends Entity> List<OTHER_ENTITY> createEntityList(MultiReferenceEditValue editValue, PrimitiveIterator.OfInt referenceIterator, EntityBuilder<OTHER_ENTITY> entityBuilder) {
		Map<RecordReference, OTHER_ENTITY> entityByReference = (Map<RecordReference, OTHER_ENTITY>) entityChangeSet.getEntityByReference();
		List<OTHER_ENTITY> list = new ArrayList<>();
		if (!editValue.getSetReferences().isEmpty() || editValue.isRemoveAll()) {
			List<RecordReference> references = editValue.isRemoveAll() ? editValue.getAddReferences() : editValue.getSetReferences();
			references.forEach(reference -> {
				OTHER_ENTITY entity = entityByReference.get(reference);
				if (entity == null && reference.getRecordId() > 0) {
					entity = entityBuilder.build(reference.getRecordId());
				}
				if (entity != null) {
					list.add(entity);
				} else {
					log.error("Cannot add reference to list: no record id and no matching correlation id!");
				}
			});
			return list;
		} else {
			Set<Integer> removeSet = new HashSet<>();
			editValue.getRemoveReferences().forEach(reference -> {
				if (reference.getRecordId() > 0) {
					removeSet.add(reference.getRecordId());
				} else {
					OTHER_ENTITY entity = entityByReference.get(reference);
					if (entity != null) {
						removeSet.add(entity.getId());
					}
				}
			});
			List<OTHER_ENTITY> addEntities = new ArrayList<>();
			editValue.getAddReferences().forEach(reference -> {
				OTHER_ENTITY entity = entityByReference.get(reference);
				if (entity == null && reference.getRecordId() > 0) {
					entity = entityBuilder.build(reference.getRecordId());
				}
				if (entity != null) {
					addEntities.add(entity);
				}
			});
			Set<Integer> addEntitySet = new HashSet<>();
			addEntities.forEach(entity -> addEntitySet.add(entity.getId()));
			if (referenceIterator != null) {
				while (referenceIterator.hasNext()) {
					int recordId = referenceIterator.nextInt();
					if (!removeSet.contains(recordId) && !addEntitySet.contains(recordId)) {
						list.add(entityBuilder.build(recordId));
					}
				}
			}
			list.addAll(addEntities);
			return list;
		}
	}

	protected TransactionRecordValue getChangeValue(ColumnIndex index) {
		if (entityChangeSet == null) {
			return null;
		} else {
			return entityChangeSet.getChangeValue(index);
		}
	}

	protected Object getChangedValue(ColumnIndex index) {
		if (entityChangeSet == null) {
			return null;
		} else {
			return entityChangeSet.getChangeValue(index).getValue();
		}
	}

	protected AbstractUdbEntity getReferenceChangeValue(ColumnIndex index) {
		if (entityChangeSet == null) {
			return null;
		} else {
			return entityChangeSet.getReferenceChange(index);
		}
	}

	protected void addMultiReferenceValue(List<? extends Entity> entities, MultiReferenceIndex multiReferenceIndex, TableIndex tableIndex) {
		if (entities == null || entities.isEmpty()) {
			return;
		}
		MultiReferenceEditValue editValue = getOrCreateMultiReferenceEditValue(multiReferenceIndex, tableIndex);
		List<RecordReference> references = createRecordReferences(entities);

		editValue.addReferences(references);
	}

	protected void removeMultiReferenceValue(List<? extends Entity> entities, MultiReferenceIndex multiReferenceIndex, TableIndex tableIndex) {
		if (entities == null || entities.isEmpty()) {
			return;
		}
		MultiReferenceEditValue editValue = getOrCreateMultiReferenceEditValue(multiReferenceIndex, tableIndex);
		List<RecordReference> references = createRecordReferences(entities);
		editValue.removeReferences(references);
	}

	protected void setMultiReferenceValue(List<? extends Entity> entities, MultiReferenceIndex multiReferenceIndex, TableIndex tableIndex) {
		if (entities == null || entities.isEmpty()) {
			return;
		}
		MultiReferenceEditValue editValue = getOrCreateMultiReferenceEditValue(multiReferenceIndex, tableIndex);
		List<RecordReference> references = createRecordReferences(entities);
		editValue.setReferences(references);
	}

	protected void removeAllMultiReferenceValue(MultiReferenceIndex multiReferenceIndex, TableIndex tableIndex) {
		MultiReferenceEditValue editValue = getOrCreateMultiReferenceEditValue(multiReferenceIndex, tableIndex);
		editValue.setRemoveAll();
	}

	private List<RecordReference> createRecordReferences(List<? extends Entity> entities) {
		List<RecordReference> references = new ArrayList<>();
		for (Entity entity : entities) {
			AbstractUdbEntity udbEntity = (AbstractUdbEntity) entity;
			RecordReference recordReference = new RecordReference(udbEntity.getId(), udbEntity.getCorrelationId());
			references.add(recordReference);
			entityChangeSet.addRecordReference(recordReference, entity);
		}
		return references;
	}

	private MultiReferenceEditValue getOrCreateMultiReferenceEditValue(MultiReferenceIndex multiReferenceIndex, TableIndex tableIndex) {
		MultiReferenceEditValue editValue;
		TransactionRecordValue changeValue = getChangeValue(multiReferenceIndex);
		if (changeValue != null) {
			editValue = (MultiReferenceEditValue) changeValue.getValue();
		} else {
			editValue = new MultiReferenceEditValue();
			setChangeValue(multiReferenceIndex, editValue, tableIndex);
		}
		return editValue;
	}


	protected boolean isChanged(ColumnIndex index) {
		return entityChangeSet != null && entityChangeSet.isChanged(index);
	}

	protected int getEntityId(Entity entity) {
		if (entity == null) {
			return 0;
		} else {
			return entity.getId();
		}
	}

	protected Transaction getTransaction() {
		return transaction;
	}

	@Override
	public void clearChanges() {
		entityChangeSet = null;
	}

	@Override
	public boolean isModified() {
		return entityChangeSet != null;
	}

	private void checkChangeSet() {
		if (entityChangeSet == null) {
			entityChangeSet = new EntityChangeSet();
			createCorrelationId();
		}
	}

	public void save(Transaction transaction, TableIndex tableIndex, boolean strictChangeVerification) {
		if (entityChangeSet != null) {
			this.transaction = transaction;
			boolean update = !createEntity;
			TransactionRecord transactionRecord = new TransactionRecord(tableIndex, id, correlationId, transaction.getUserId(), update, false, strictChangeVerification);
			entityChangeSet.setTransactionRecordValues(transaction, transactionRecord, strictChangeVerification);
			transaction.addTransactionRecord(transactionRecord);
			clearChanges();
		}
	}

	public void save(TableIndex tableIndex) {
		save(tableIndex, false);
	}

	public CompletableFuture<Boolean> saveAsynchronously(TableIndex tableIndex) {
		save(tableIndex, true);
		return new CompletableFuture<>();
	}

	public void save(TableIndex tableIndex, boolean asynchronous) {
		if (entityChangeSet != null) {
			transaction = Transaction.create();
			save(transaction, tableIndex, false);
			transaction.execute(asynchronous);
			if (id == 0) {
				id = transaction.getResolvedRecordIdByCorrelationId(correlationId);
			}
		}
	}

	protected TableIndex getTableIndex() {
		return tableIndex;
	}

	public void delete(Transaction transaction, TableIndex tableIndex) {
		TransactionRecord transactionRecord = new TransactionRecord(tableIndex, id, 0, transaction.getUserId(), true);
		transaction.addTransactionRecord(transactionRecord);
		clearChanges();
	}

	public void delete(TableIndex tableIndex) {
		Transaction transaction = Transaction.create();
		delete(transaction, tableIndex);
		transaction.execute();
	}

	@Override
	public boolean isCommitted() {
		return entityChangeSet == null;
	}

	@Override
	public boolean isStored() {
		return id > 0 && !createEntity;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AbstractUdbEntity<?> that = (AbstractUdbEntity<?>) o;
		if (getId() > 0 && getId() == that.getId()) {
			return true;
		}
		return getCorrelationId() > 0 && getCorrelationId() == that.getCorrelationId();
	}

	@Override
	public int hashCode() {
		if (getId() > 0) {
			return getId();
		} else {
			return getCorrelationId();
		}
	}
}
