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
package org.teamapps.universaldb.index.reference.multi;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.buffer.BlockChainAtomicStore;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.ReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.reference.value.*;
import org.teamapps.universaldb.transaction.DataType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.teamapps.universaldb.index.IndexType.MULTI_REFERENCE;

public class MultiReferenceIndex extends AbstractIndex<MultiReferenceValue, MultiReferenceFilter> implements ReferenceIndex {

	private final BlockChainAtomicStore referenceStore;
	private TableIndex referencedTable;
	private boolean cyclicReferences;
	private boolean cascadeDeleteReferences;
	private SingleReferenceIndex reverseSingleIndex;
	private MultiReferenceIndex reverseMultiIndex;

	private boolean ensureNoDuplicates = true;

	public MultiReferenceIndex(String name, TableIndex table, ColumnType columnType) {
		super(name, table, columnType, FullTextIndexingOptions.NOT_INDEXED);
		this.referenceStore = new BlockChainAtomicStore(table.getDataPath(), name);
	}

	public void setReferencedTable(TableIndex referencedTable, ColumnIndex reverseIndex, boolean cascadeDeleteReferences) {
		this.referencedTable = referencedTable;
		if (reverseIndex != null) {
			if (reverseIndex instanceof SingleReferenceIndex) {
				reverseSingleIndex = (SingleReferenceIndex) reverseIndex;
			} else {
				reverseMultiIndex = (MultiReferenceIndex) reverseIndex;
			}
			cyclicReferences = true;
		}
		this.cascadeDeleteReferences = cascadeDeleteReferences;
	}

	@Override
	public IndexType getType() {
		return MULTI_REFERENCE;
	}

	public TableIndex getReferencedTable() {
		return referencedTable;
	}

	@Override
	public boolean isCascadeDeleteReferences() {
		return cascadeDeleteReferences;
	}

	@Override
	public boolean isMultiReference() {
		return true;
	}

	@Override
	public ColumnIndex getReferencedColumn() {
		if (reverseSingleIndex != null) {
			return reverseSingleIndex;
		} else {
			return reverseMultiIndex;
		}
	}

	@Override
	public MultiReferenceValue getGenericValue(int id) {
		List<Integer> entries = referenceStore.getEntries(id);
		return entries != null ? new ReferenceIteratorValue(entries) : null;
	}

	@Override
	public void setGenericValue(int id, MultiReferenceValue value) {
		switch (value.getType()) {
			case REFERENCE_ITERATOR:
				break;
			case EDIT_VALUE:
				setReferenceEditValue(id, (MultiReferenceEditValue) value);
				break;
		}
	}

	@Override
	public void removeValue(int id) {
		removeAllReferences(id, false);
	}

	@Override
	public boolean isEmpty(int id) {
		return referenceStore.isEmpty(id);
	}

	public int getReferencesCount(int id) {
		return  referenceStore.getEntryCount(id);
	}

	public List<Integer> getReferencesAsList(int id) {
		return referenceStore.getEntries(id);
	}

	public boolean containsReference(int id, int reference) {
		return referenceStore.containsEntry(id, reference);
	}

	public boolean containsReference(int id, BitSet reference) {
		return referenceStore.containsEntry(id, reference);
	}

	public BitSet getReferencesAsBitSet(int id) {
		BitSet bitSet = new BitSet();
		List<Integer> entries = referenceStore.getEntries(id);
		if (entries != null) {
			entries.forEach(bitSet::set);
		}
		return bitSet;
	}

	public List<CyclicReferenceUpdate> setReferenceEditValue(int id, MultiReferenceEditValue editValue) {
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();
		if (!editValue.getSetReferences().isEmpty()) {
			List<Integer> references = RecordReference.createRecordIdsList(editValue.getSetReferences());
			cyclicReferenceUpdates.addAll(setReferences(id, references, false));
		} else if (editValue.isRemoveAll()) {
			cyclicReferenceUpdates.addAll(removeAllReferences(id, false));
			if (!editValue.getAddReferences().isEmpty()) {
				List<Integer> references = RecordReference.createRecordIdsList(editValue.getAddReferences());
				cyclicReferenceUpdates.addAll(addReferences(id, references, false));
			}
		} else {
			if (!editValue.getRemoveReferences().isEmpty()) {
				List<Integer> references = RecordReference.createRecordIdsList(editValue.getRemoveReferences());
				cyclicReferenceUpdates.addAll(removeReferences(id, references, false));
			}
			if (!editValue.getAddReferences().isEmpty()) {
				List<Integer> references = RecordReference.createRecordIdsList(editValue.getAddReferences());
				cyclicReferenceUpdates.addAll(addReferences(id, references, false));
			}
		}
		return cyclicReferenceUpdates;
	}

	public List<CyclicReferenceUpdate> setReferences(int id, List<Integer> references, boolean cyclic) {
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();
		if (cyclicReferences && !cyclic) {
			List<Integer> previousEntries = referenceStore.getEntries(id);
			if (!previousEntries.isEmpty()) {
				removeCyclicReferences(id, previousEntries, cyclicReferenceUpdates);
			}
		}
		referenceStore.setEntries(id, references);
		if (cyclicReferences && !cyclic) {
			addCyclicReferences(id, references, cyclicReferenceUpdates);
		}
		return cyclicReferenceUpdates;
	}

	public List<CyclicReferenceUpdate> addReferences(int id, List<Integer> references, boolean cyclic) {
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();
		if (ensureNoDuplicates && !referenceStore.isEmpty(id)) {
			Set<Integer> existingSet = new HashSet<>(referenceStore.getEntries(id));
			references = references.stream().filter(value -> !existingSet.contains(value)).collect(Collectors.toList());
		}
		referenceStore.addEntries(id, references);
		if (cyclicReferences && !cyclic) {
			addCyclicReferences(id, references, cyclicReferenceUpdates);
		}
		return cyclicReferenceUpdates;
	}

	public List<CyclicReferenceUpdate> removeReferences(int id, List<Integer> references, boolean cyclic) {
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();
		if (referenceStore.isEmpty(id)) {
			return cyclicReferenceUpdates;
		}
		referenceStore.removeEntries(id, references);
		if (cyclicReferences && !cyclic) {
			removeCyclicReferences(id, references, cyclicReferenceUpdates);
		}
		return cyclicReferenceUpdates;
	}

	public List<CyclicReferenceUpdate> removeAllReferences(int id, boolean cyclic) {
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();
		if (referenceStore.isEmpty(id)) {
			return cyclicReferenceUpdates;
		}
		if (cyclic || !cyclicReferences) {
			referenceStore.removeAllEntries(id);
		} else {
			List<Integer> removeEntries = referenceStore.getEntries(id);
			referenceStore.removeAllEntries(id);
			removeCyclicReferences(id, removeEntries, cyclicReferenceUpdates);
		}
		return cyclicReferenceUpdates;
	}

	private void addCyclicReferences(int id, List<Integer> references, List<CyclicReferenceUpdate> cyclicReferenceUpdates) {
		if (reverseSingleIndex != null) {
			for (Integer reference : references) {
				int previousValue = reverseSingleIndex.getValue(reference);
				if (previousValue > 0 && previousValue != id) {
					removeReferences(previousValue, Collections.singletonList(reference), true);
				}
				reverseSingleIndex.setIndexValue(reference, id);
				cyclicReferenceUpdates.add(new CyclicReferenceUpdate(reverseSingleIndex, false, reference, id));
			}
		} else {
			for (Integer reference : references) {
				reverseMultiIndex.addReferences(reference, Collections.singletonList(id), true);
				cyclicReferenceUpdates.add(new CyclicReferenceUpdate(reverseMultiIndex, false, reference, id));
			}
		}
	}

	private void removeCyclicReferences(int id, List<Integer> references, List<CyclicReferenceUpdate> cyclicReferenceUpdates) {
		if (reverseSingleIndex != null) {
			for (Integer reference : references) {
				int previousValue = reverseSingleIndex.getValue(reference);
				if (id == previousValue) {
					reverseSingleIndex.setIndexValue(reference, 0);
					cyclicReferenceUpdates.add(new CyclicReferenceUpdate(reverseSingleIndex, true, reference, 0));
				}
			}
		} else {
			for (Integer reference : references) {
				reverseMultiIndex.removeReferences(reference, Collections.singletonList(id), true);
				cyclicReferenceUpdates.add(new CyclicReferenceUpdate(reverseMultiIndex, true, reference, 0));
			}
		}
	}

	@Override
	public void writeTransactionValue(MultiReferenceValue value, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.MULTI_REFERENCE.getId());
		dataOutputStream.writeInt(value.getType().getId());
		value.writeValues(dataOutputStream);
	}

	@Override
	public MultiReferenceValue readTransactionValue(DataInputStream dataInputStream) throws IOException {
		return MultiReferenceValue.create(dataInputStream);
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext) {
		int order = ascending ? 1 : -1;
		sortEntries.sort((o1, o2) -> {
			int value1 = getReferencesCount(o1.getLeafId());
			int value2 = getReferencesCount(o2.getLeafId());
			return Integer.compare(value1, value2) * order;
		});
		return sortEntries;
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			List<Integer> references = getReferencesAsList(id);
			dataOutputStream.writeInt(id);
			dataOutputStream.writeInt(references.size());
			for (Integer reference : references) {
				dataOutputStream.writeInt(reference);
			}
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		boolean cyclicReferencesValue = this.cyclicReferences;
		this.cyclicReferences = false;
		try {
			int id = dataInputStream.readInt();
			int count = dataInputStream.readInt();
			List<Integer> references = new ArrayList<>();
			for (int i = 0; i < count; i++) {
				references.add(dataInputStream.readInt());
			}
			setReferences(id, references, true);
		} catch (EOFException ignore) { } finally {
			this.cyclicReferences = cyclicReferencesValue;
		}
	}

	@Override
	public BitSet filter(BitSet records, MultiReferenceFilter filter) {
		switch (filter.getType()) {
			case EQUALS:
				return filterEquals(records, filter.getReferencesSet());
			case NOT_EQUALS:
				return filterNotEquals(records, filter.getReferencesSet());
			case IS_EMPTY:
				return filterIsEmpty(records);
			case IS_NOT_EMPTY:
				return filterIsNotEmpty(records);
			case CONTAINS_ANY:
				return filterContainsAny(records, filter.getReferencesSet());
			case CONTAINS_NONE:
				return filterContainsNone(records, filter.getReferencesSet());
			case CONTAINS_ALL:
				return filterContainsAll(records, filter.getReferencesSet());
			case CONTAINS_ANY_NOT:
				return filterContainsAnyNot(records, filter.getReferencesSet());
			case ENTRY_COUNT_EQUALS:
				return filterEntryCountEquals(records, filter.getCountFilter());
			case ENTRY_COUNT_GREATER:
				return filterEntryCountGreater(records, filter.getCountFilter());
			case ENTRY_COUNT_LESSER:
				return filterEntryCountSmaller(records, filter.getCountFilter());
		}
		return null;
	}

	@Override
	public void close() {
		referenceStore.close();
	}

	@Override
	public void drop() {
		referenceStore.drop();
	}

	public BitSet filterEquals(BitSet bitSet, Set<Integer> compareIds) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int count = referenceStore.getEntryCount(id);
			if (count == compareIds.size()) {
				HashSet<Integer> referenceSet = new HashSet<>(referenceStore.getEntries(id));
				if (referenceSet.equals(compareIds)) {
					result.set(id);
				}
			}
		}
		return result;
	}

	public BitSet filterNotEquals(BitSet bitSet, Set<Integer> compareIds) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int count = referenceStore.getEntryCount(id);
			if (count != compareIds.size()) {
				result.set(id);
			} else {
				HashSet<Integer> referenceSet = new HashSet<>(referenceStore.getEntries(id));
				if (!referenceSet.equals(compareIds)) {
					result.set(id);
				}
			}
		}
		return result;
	}

	public BitSet filterIsEmpty(BitSet bitSet) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			if (referenceStore.isEmpty(id)) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterIsNotEmpty(BitSet bitSet) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			if (!referenceStore.isEmpty(id)) {
				result.set(id);
			}
		}
		return result;
	}

	private BitSet filterContainsAny(BitSet bitSet, Set<Integer> compareIds) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			List<Integer> entries = referenceStore.getEntries(id);
			for (Integer entry : entries) {
				if (compareIds.contains(entry)) {
					result.set(id);
					break;
				}
			}
		}
		return result;
	}

	private BitSet filterContainsNone(BitSet bitSet, Set<Integer> compareIds) {
		BitSet result = filterContainsAny(bitSet, compareIds);
		return negateInput(bitSet, result);
	}

	public BitSet filterContainsAll(BitSet bitSet, Set<Integer> compareIds) {
		BitSet result = new BitSet();
		BitSet compareSet = new BitSet();
		compareIds.forEach(compareSet::set);
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			BitSet referencesAsBitSet = getReferencesAsBitSet(id);
			referencesAsBitSet.and(compareSet);
			if (referencesAsBitSet.equals(compareSet)) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterContainsAnyNot(BitSet bitSet, Set<Integer> compareIds) {
		BitSet containsAll = filterContainsAll(bitSet, compareIds);
		return negateInput(bitSet, containsAll);
	}

	public BitSet filterEntryCountEquals(BitSet bitSet, int count) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			if (referenceStore.getEntryCount(id) == count) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterEntryCountGreater(BitSet bitSet, int count) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			if (referenceStore.getEntryCount(id) > count) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterEntryCountSmaller(BitSet bitSet, int count) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			if (referenceStore.getEntryCount(id) < count) {
				result.set(id);
			}
		}
		return result;
	}

}
