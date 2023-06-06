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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.FieldIndex;
import org.teamapps.universaldb.index.SortEntry;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.bool.BooleanIndex;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.file.store.FileStore;
import org.teamapps.universaldb.index.numeric.*;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.reference.value.*;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.index.transaction.request.TransactionRequest;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecord;
import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordValue;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.index.translation.TranslatableTextIndex;
import org.teamapps.universaldb.index.versioning.RecordUpdate;
import org.teamapps.universaldb.record.EntityBuilder;
import org.teamapps.universaldb.schema.Table;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class AbstractUdbEntity<ENTITY extends Entity> implements Entity<ENTITY>, EntityBuilder<ENTITY> {

	private static final Logger log = LoggerFactory.getLogger(AbstractUdbEntity.class);
	private static final AtomicInteger correlationIdGenerator = new AtomicInteger();
	private static final int MAX_CORRELATION_ID = 2_000_000_000;

	private final TableIndex tableIndex;
	private int id;
	private boolean createEntity;
	private int correlationId;
	private EntityChangeSet entityChangeSet;
	private TransactionRequest transactionRequest;

	public static <ENTITY> List<ENTITY> createEntityList(EntityBuilder<ENTITY> entityBuilder, List<Integer> recordIds){
		List<ENTITY> list = new ArrayList<>();
		for (Integer recordId : recordIds) {
			list.add(entityBuilder.build(recordId));
		}
		return list;
	}

	public static FileValue createFileValue(File file) {
		return createFileValue(file, null);
	}

	public static FileValue createFileValue(File file, String fileName) {
		return file != null ? FileValue.create(file, fileName) : null;
	}


	public static <ENTITY extends Entity> List<ENTITY> sort(TableIndex table, List<ENTITY> list, String sortFieldName, boolean ascending, UserContext userContext, String ... path) {
		SingleReferenceIndex[] referencePath = getReferenceIndices(table, path);
		FieldIndex column = getSortColumn(table, sortFieldName, referencePath);
		if (column == null) {
			return list;
		}
		List<SortEntry<ENTITY>> sortEntries = SortEntry.createSortEntries(list, referencePath);
		sortEntries = column.sortRecords(sortEntries, ascending, userContext);
		return sortEntries.stream().map(SortEntry::getEntity).collect(Collectors.toList());
	}

	public static <ENTITY extends Entity> List<ENTITY> sort(TableIndex table, EntityBuilder<ENTITY> builder, BitSet recordIds, String sortFieldName, boolean ascending, UserContext userContext, String ... path) {
		SingleReferenceIndex[] referencePath = getReferenceIndices(table, path);
		FieldIndex column = getSortColumn(table, sortFieldName, referencePath);
		if (column == null) {
			return createUnsortedList(recordIds, builder);
		}
		List<SortEntry> sortEntries = SortEntry.createSortEntries(recordIds, referencePath);
		sortEntries = column.sortRecords(sortEntries, ascending, userContext);
		List<ENTITY> list = new ArrayList<>();
		for (SortEntry entry : sortEntries) {
			list.add(builder.build(entry.getId()));
		}
		return list;
	}

	private static <ENTITY extends Entity> List<ENTITY> createUnsortedList(BitSet records, EntityBuilder<ENTITY> builder) {
		List<ENTITY> list = new ArrayList<>();
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			list.add(builder.build(id));
		}
		return list;
	}

	private static SingleReferenceIndex[] getReferenceIndices(TableIndex table, String[] path) {
		SingleReferenceIndex[] referencePath = null;
		if (path != null && path.length > 0) {
			referencePath = new SingleReferenceIndex[path.length];
			TableIndex pathTable = table;
			for (int i = 0; i < path.length; i++) {
				referencePath[i] = (SingleReferenceIndex) pathTable.getFieldIndex(path[i]);
				pathTable = referencePath[i].getReferencedTable();
			}
		}
		return referencePath;
	}

	private static FieldIndex getSortColumn(TableIndex table, String sortFieldName, SingleReferenceIndex[] referencePath) {
		FieldIndex column;
		if (referencePath != null && referencePath.length > 0) {
			column = referencePath[referencePath.length - 1].getReferencedTable().getFieldIndex(sortFieldName);
		} else {
			column = table.getFieldIndex(sortFieldName);
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
		if (id == 0 && transactionRequest != null) {
			id = transactionRequest.getResolvedRecordIdByCorrelationId(correlationId);
		}
		return id;
	}

	public int createdBy() {
		FieldIndex fieldIndex = tableIndex.getFieldIndex(Table.FIELD_CREATED_BY);
		return fieldIndex != null ? getIntValue((IntegerIndex) fieldIndex) : 0;
	}



	public List<RecordUpdate> getRecordUpdates() {
		try {
			return tableIndex.getRecordVersioningIndex().readRecordUpdates(id);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected int getCorrelationId() {
		return correlationId;
	}

	public Object getEntityValue(String fieldName) {
		if (Table.FIELD_ID.equals(fieldName)) {
			return getId();
		}
		FieldIndex index = tableIndex.getFieldIndex(fieldName);
		if (index != null) {
			return index.getGenericValue(getId());
		} else {
			return null;
		}
	}

	public void setEntityValue(String fieldName, Object value) {
		FieldIndex index = tableIndex.getFieldIndex(fieldName);
		if (index != null) {
			setChangeValue(index, value, null);
		}
	}

	protected void setChangeValue(FieldIndex index, Object value, TableIndex tableIndex) {
		checkChangeSet();
		entityChangeSet.addChangeValue(index, value);
	}

	protected void setSingleReferenceValue(SingleReferenceIndex index, Entity reference, TableIndex tableIndex) {
		AbstractUdbEntity entity = (AbstractUdbEntity) reference;
		RecordReference recordReference = null;
		if (entity != null) {
			recordReference = new RecordReference(entity.getId(), entity.getCorrelationId());
		}
		int currentValue = index.getValue(getId());
		if ((currentValue == 0 && entity != null) || (currentValue > 0 && entity == null) || (entity != null && (entity.getId() == 0 || entity.getId() != currentValue))) {
			checkChangeSet();
			entityChangeSet.addChangeValue(index, recordReference);
			entityChangeSet.setReferenceChange(index, entity);
		}
	}

	protected <OTHER_ENTITY extends Entity> List<OTHER_ENTITY> createEntityList(FieldIndex index, EntityBuilder<OTHER_ENTITY> entityBuilder) {
		TransactionRequestRecordValue changeValue = getChangeValue(index);
		MultiReferenceEditValue editValue = (MultiReferenceEditValue) changeValue.getValue();
		MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) index;
		return createEntityList(editValue, multiReferenceIndex.getReferencesAsList(id), entityBuilder);
	}

	protected <OTHER_ENTITY extends Entity> List<OTHER_ENTITY> createEntityList(MultiReferenceEditValue editValue, List<Integer> referencedRecords, EntityBuilder<OTHER_ENTITY> entityBuilder) {
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
			for (Integer recordId : referencedRecords) {
				if (!removeSet.contains(recordId) && !addEntitySet.contains(recordId)) {
					list.add(entityBuilder.build(recordId));
				}
			}
			list.addAll(addEntities);
			return list;
		}
	}

	protected TransactionRequestRecordValue getChangeValue(FieldIndex index) {
		if (entityChangeSet == null) {
			return null;
		} else {
			return entityChangeSet.getChangeValue(index);
		}
	}

	protected Object getChangedValue(FieldIndex index) {
		if (entityChangeSet == null) {
			return null;
		} else {
			return entityChangeSet.getChangeValue(index).getValue();
		}
	}

	protected AbstractUdbEntity getReferenceChangeValue(FieldIndex index) {
		if (entityChangeSet == null) {
			return null;
		} else {
			return entityChangeSet.getReferenceChange(index.getMappingId());
		}
	}

	protected void addMultiReferenceValue(List<? extends Entity> entities, MultiReferenceIndex multiReferenceIndex) {
		if (entities == null || entities.isEmpty()) {
			return;
		}
		MultiReferenceEditValue editValue = getOrCreateMultiReferenceEditValue(multiReferenceIndex);
		List<RecordReference> references = createRecordReferences(entities);

		editValue.addReferences(references);
	}

	protected void removeMultiReferenceValue(List<? extends Entity> entities, MultiReferenceIndex multiReferenceIndex) {
		if (entities == null || entities.isEmpty()) {
			return;
		}
		MultiReferenceEditValue editValue = getOrCreateMultiReferenceEditValue(multiReferenceIndex);
		List<RecordReference> references = createRecordReferences(entities);
		editValue.removeReferences(references);
	}

	protected void setMultiReferenceValue(List<? extends Entity> entities, MultiReferenceIndex multiReferenceIndex) {
		if (entities == null || entities.isEmpty()) {
			if (getChangeValue(multiReferenceIndex) != null || !multiReferenceIndex.isEmpty(getId())) {
				removeAllMultiReferenceValue(multiReferenceIndex);
			}
		} else {
			if (getChangeValue(multiReferenceIndex) == null && multiReferenceIndex.getReferencesCount(getId()) == entities.size() && entities.stream().allMatch(Entity::isStored)) {
				Set<Integer> entityIdSet = entities.stream().map(Entity::getId).collect(Collectors.toSet());
				if (entityIdSet.containsAll(multiReferenceIndex.getReferencesAsList(getId()))) {
					return;
				}
			}
			MultiReferenceEditValue editValue = getOrCreateMultiReferenceEditValue(multiReferenceIndex);
			List<RecordReference> references = createRecordReferences(entities);
			editValue.setReferences(references);
		}
	}

	protected void removeAllMultiReferenceValue(MultiReferenceIndex multiReferenceIndex) {
		MultiReferenceEditValue editValue = getOrCreateMultiReferenceEditValue(multiReferenceIndex);
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

	private MultiReferenceEditValue getOrCreateMultiReferenceEditValue(MultiReferenceIndex multiReferenceIndex) {
		MultiReferenceEditValue editValue;
		TransactionRequestRecordValue changeValue = getChangeValue(multiReferenceIndex);
		if (changeValue != null) {
			editValue = (MultiReferenceEditValue) changeValue.getValue();
		} else {
			editValue = new MultiReferenceEditValue();
			setChangeValue(multiReferenceIndex, editValue, tableIndex);
		}
		return editValue;
	}

	public boolean getBooleanValue(BooleanIndex index) {
		if (isChanged(index)) {
			return (boolean) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setBooleanValue(boolean value, BooleanIndex index) {
		if (getBooleanValue(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public short getShortValue(ShortIndex index) {
		if (isChanged(index)) {
			return (short) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setShortValue(short value, ShortIndex index) {
		if (getShortValue(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public int getIntValue(IntegerIndex index) {
		if (isChanged(index)) {
			return (int) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setIntValue(int value, IntegerIndex index) {
		if (getIntValue(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public long getLongValue(LongIndex index) {
		if (isChanged(index)) {
			return (long) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setLongValue(long value, LongIndex index) {
		if (getLongValue(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public float getFloatValue(FloatIndex index) {
		if (isChanged(index)) {
			return (float) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setFloatValue(float value, FloatIndex index) {
		if (getFloatValue(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public double getDoubleValue(DoubleIndex index) {
		if (isChanged(index)) {
			return (double) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setDoubleValue(double value, DoubleIndex index) {
		if (getDoubleValue(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public String getTextValue(TextIndex index) {
		if (isChanged(index)) {
			return (String) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setTextValue(String value, TextIndex index) {
		if (value != null && value.isEmpty()) {
			value = null;
		}
		if (!Objects.equals(getTextValue(index), value)) {
 			setChangeValue(index, value, tableIndex);
		}
	}

	public TranslatableText getTranslatableTextValue(TranslatableTextIndex index) {
		if (isChanged(index)) {
			return (TranslatableText) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setTranslatableTextValue(TranslatableText value, TranslatableTextIndex index) {
		setChangeValue(index, value, tableIndex);
	}

	public Instant getTimestampValue(IntegerIndex index) {
		int value;
		if (isChanged(index)) {
			value = (int) getChangedValue(index);
		} else {
			value = index.getValue(getId());
		}
		return value == 0 ? null : Instant.ofEpochSecond(value);
	}

	public int getTimestampAsEpochSecond(IntegerIndex index) {
		if (isChanged(index)) {
			return (int) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public long getTimestampAsEpochMilli(IntegerIndex index) {
		if (isChanged(index)) {
			return ((Integer) getChangedValue(index)).longValue() * 1000L;
		} else {
			return index.getValue(getId()) * 1000L;
		}
	}

	public void setTimestampValue(Instant value, IntegerIndex index) {
		if (!Objects.equals(getTimestampValue(index), value)) {
			Integer intValue = value != null ? (int) value.getEpochSecond() : 0;
			setChangeValue(index, intValue, tableIndex);
		}
	}

	public void setTimestampAsEpochSecond(int value, IntegerIndex index) {
		if (getTimestampAsEpochSecond(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public void setTimestampAsEpochMilli(long value, IntegerIndex index) {
		if (getTimestampAsEpochMilli(index) != value) {
			int intValue = (int) (value / 1000);
			setChangeValue(index, intValue, tableIndex);
		}
	}

	public Instant getTimeValue(IntegerIndex index) {
		int value;
		if (isChanged(index)) {
			value = (int) getChangedValue(index);
		} else {
			value = index.getValue(getId());
		}
		return value == 0 ? null: Instant.ofEpochSecond(value);
	}

	public void setTimeValue(Instant value, IntegerIndex index) {
		if (!Objects.equals(getTimeValue(index), value)) {
			Integer intValue = value != null ? (int) value.getEpochSecond() : 0;
			setChangeValue(index, intValue, tableIndex);
		}
	}

	public Instant getDateValue(LongIndex index) {
		long value;
		if (isChanged(index)) {
			value = (long) getChangedValue(index);
		} else {
			value = index.getValue(getId());
		}
		return value == 0 ? null : Instant.ofEpochMilli(value);
	}

	public long getDateAsEpochMilli(LongIndex index) {
		if (isChanged(index)) {
			return (long) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setDateValue(Instant value, LongIndex index) {
		if (!Objects.equals(getDateValue(index), value)) {
			Long longValue = value != null ? value.toEpochMilli() : 0;
			setChangeValue(index, longValue, tableIndex);
		}
	}

	public void setDateAsEpochMilli(long value, LongIndex index) {
		if (getDateAsEpochMilli(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public Instant getDateTimeValue(LongIndex index) {
		long value;
		if (isChanged(index)) {
			value = (long) getChangedValue(index);
		} else {
			value = index.getValue(getId());
		}
		return value == 0 ? null : Instant.ofEpochMilli(value);
	}

	public long getDateTimeAsEpochMilli(LongIndex index) {
		if (isChanged(index)) {
			return (long) getChangedValue(index);
		} else {
			return index.getValue(getId());
		}
	}

	public void setDateTimeValue(Instant value, LongIndex index) {
		if (!Objects.equals(getDateTimeValue(index), value)) {
			Long longValue = value != null ? (long) value.toEpochMilli() : 0;
			setChangeValue(index, longValue, tableIndex);
		}
	}

	public void setDateTimeAsEpochMilli(long value, LongIndex index) {
		if (getDateTimeAsEpochMilli(index) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public LocalDate getLocalDateValue(LongIndex index) {
		long value;
		if (isChanged(index)) {
			value = (long) getChangedValue(index);
		} else {
			value = index.getValue(getId());
		}
		return value == 0 ? null : LocalDate.ofInstant(Instant.ofEpochMilli(value), ZoneOffset.UTC);
	}

	public void setLocalDateValue(LocalDate value, LongIndex index) {
		long longValue = value != null ? value.atStartOfDay(ZoneOffset.UTC).toEpochSecond() * 1000L : 0;
		LocalDate currentValue = getLocalDateValue(index);
		if (!Objects.equals(currentValue, value) && (currentValue != null || longValue != 0)) {
			setChangeValue(index, longValue, tableIndex);
		}
	}

	public void setLocalDateAsEpochMilli(long value, LongIndex index) {
		if (index.getValue(getId()) != value) {
			setChangeValue(index, value, tableIndex);
		}
	}

	public <ENUM extends Enum<ENUM>> ENUM getEnumValue(ShortIndex index, ENUM[] values) {
		short shortValue;
		if (isChanged(index)) {
			shortValue = (short) getChangedValue(index);
		} else {
			shortValue = index.getValue(getId());
		}
		return shortValue == 0 ? null : values[shortValue - 1];
	}

	public <ENUM extends Enum<ENUM>> void setEnumValue(ShortIndex index, ENUM value) {
		short shortValue = (short) (value != null ? value.ordinal() + 1 : 0);
		if (getShortValue(index) != shortValue) {
			setChangeValue(index, shortValue, tableIndex);
		}
	}

	public <OTHER_ENTITY extends Entity> List<OTHER_ENTITY> getMultiReferenceValue(MultiReferenceIndex index, EntityBuilder<OTHER_ENTITY> entityBuilder) {
		if (isChanged(index)) {
			return createEntityList(index, entityBuilder);
		} else {
			if (!index.isEmpty(getId())) {
				return createEntityList(entityBuilder, index.getReferencesAsList(getId()));
			} else {
				return Collections.emptyList();
			}
		}
	}

	public <OTHER_ENTITY extends Entity> int getMultiReferenceValueCount(MultiReferenceIndex index, EntityBuilder<OTHER_ENTITY> entityBuilder) {
		if (isChanged(index)) {
			return createEntityList(index, entityBuilder).size();
		} else {
			return index.getReferencesCount(getId());
		}
	}

	public <OTHER_ENTITY extends Entity> BitSet getMultiReferenceValueAsBitSet(MultiReferenceIndex index, EntityBuilder<OTHER_ENTITY> entityBuilder) {
		if (isChanged(index)) {
			BitSet bitSet = new BitSet();
			createEntityList(index, entityBuilder).stream().map(Entity::getId).forEach(bitSet::set);
			return bitSet;
		} else {
			return index.getReferencesAsBitSet(getId());
		}
	}

	public boolean isChanged(FieldIndex index) {
		return entityChangeSet != null && entityChangeSet.isChanged(index);
	}

	@Override
	public void clearChanges() {
		entityChangeSet = null;
	}

	@Override
	public boolean isChanged(String fieldName) {
		return isChanged(tableIndex.getFieldIndex(fieldName));
	}

	@Override
	public void clearFieldChanges(String fieldName) {
		if (entityChangeSet != null) {
			entityChangeSet.removeChange(tableIndex.getFieldIndex(fieldName));
		}
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

	public void saveRecord(UniversalDB database) {
		if (entityChangeSet != null) {
			this.transactionRequest = database.createTransactionRequest();
			saveRecord(transactionRequest, database);
			database.executeTransaction(transactionRequest);
			if (id == 0) {
				id = transactionRequest.getResolvedRecordIdByCorrelationId(correlationId);
			}
		}
	}

	public void saveRecord(TransactionRequest transactionRequest, UniversalDB database) {
		if (entityChangeSet != null) {
			this.transactionRequest = transactionRequest;
			boolean update = !createEntity;
			TransactionRequestRecord record = TransactionRequestRecord.createOrUpdateRecord(transactionRequest, tableIndex, id, correlationId, update);
			entityChangeSet.setTransactionRequestRecordValues(transactionRequest, record, database);
			transactionRequest.addRecord(record);
			clearChanges();
			createEntity = false;
		}
	}

	public TableIndex getTableIndex() {
		return tableIndex;
	}

	@Override
	public int getTableId() {
		return tableIndex.getMappingId();
	}

	public String getQualifiedName() {
		return tableIndex.getFQN();
	}

	public void deleteRecord(UniversalDB database) {
		TransactionRequest transactionRequest = database.createTransactionRequest();
		transactionRequest.addRecord(TransactionRequestRecord.createDeleteRecord(transactionRequest, tableIndex, id));
		clearChanges();
		database.executeTransaction(transactionRequest);
	}

	public void restoreDeletedRecord(UniversalDB database) {
		TransactionRequest transactionRequest = database.createTransactionRequest();
		transactionRequest.addRecord(TransactionRequestRecord.createRestoreRecord(transactionRequest, tableIndex, id));
		database.executeTransaction(transactionRequest);
	}

	public boolean isRestorable() {
		return tableIndex.getTableModel().isRecoverableRecords();
	}

	@Override
	public boolean isStored() {
		return getId() > 0 && tableIndex.isStored(id);
	}

	@Override
	public boolean isDeleted() {
		return getId() > 0 && tableIndex.isDeleted(id);
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

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(tableIndex.getName()).append(": ").append(getId()).append("\n");
		List<FieldIndex> sortedFields = new ArrayList<>();
		sortedFields.addAll(tableIndex.getFieldIndices().stream().filter(column -> !Table.isReservedMetaName(column.getName())).toList());
		sortedFields.addAll(tableIndex.getFieldIndices().stream().filter(column -> Table.isReservedMetaName(column.getName())).toList());
		for (FieldIndex<?, ?> column : sortedFields) {
			sb.append("\t").append(column.getName()).append(": ").append(column.getStringValue(getId()));
			if (isChanged(column)) {
				TransactionRequestRecordValue changeValue = getChangeValue(column);
				sb.append(" -> ").append(changeValue.getValue());
			}
			sb.append("\n");
		}
		return sb.toString();
	}
}
