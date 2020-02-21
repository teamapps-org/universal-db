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
package org.teamapps.universaldb.index.reference.multi;

import org.agrona.collections.IntHashSet;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.index.reference.blockindex.ReferenceBlockChain;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.reference.value.*;
import org.teamapps.universaldb.transaction.DataType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.PrimitiveIterator.OfInt;

import static org.teamapps.universaldb.index.IndexType.MULTI_REFERENCE;

public class MultiReferenceIndex extends AbstractIndex<MultiReferenceValue, MultiReferenceFilter> {

	private final LongIndex entryIndex;
	private final ReferenceBlockChain referenceBlockChain;

	private TableIndex referencedTable;
	private boolean cyclicReferences;
	private SingleReferenceIndex reverseSingleIndex;
	private MultiReferenceIndex reverseMultiIndex;

	public MultiReferenceIndex(String name, TableIndex table, ReferenceBlockChain referenceBlockChain) {
		super(name, table, FullTextIndexingOptions.NOT_INDEXED);
		this.entryIndex = new LongIndex(name, table);
		this.referenceBlockChain = referenceBlockChain;
	}

	public void setReferencedTable(TableIndex referencedTable, ColumnIndex reverseIndex) {
		this.referencedTable = referencedTable;
		if (reverseIndex != null) {
			if (reverseIndex instanceof SingleReferenceIndex) {
				reverseSingleIndex = (SingleReferenceIndex) reverseIndex;
			} else {
				reverseMultiIndex = (MultiReferenceIndex) reverseIndex;
			}
			cyclicReferences = true;
		}
	}

	@Override
	public IndexType getType() {
		return MULTI_REFERENCE;
	}

	public TableIndex getReferencedTable() {
		return referencedTable;
	}

	@Override
	public MultiReferenceValue getGenericValue(int id) {
		OfInt references = getReferences(id);
		if (references == null) {
			return null;
		} else {
			return new ReferenceIteratorValue(references);
		}
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
		long index = entryIndex.getValue(id);
		if (index > 0) {
			referenceBlockChain.removeAll(index);
		}
	}

	public OfInt getReferences(int id) {
		long index = entryIndex.getValue(id);
		if (index > 0) {
			return referenceBlockChain.getReferences(index);
		} else {
			return null;
		}
	}

	public boolean isEmpty(int id) {
		if (id == 0) {
			return true;
		} else {
			return entryIndex.getValue(id) <= 0;
		}
	}

	public int getReferencesCount(int id) {
		if (id == 0) {
			return 0;
		}
		long index = entryIndex.getValue(id);
		if (index > 0) {
			return referenceBlockChain.getReferencesCount(index);
		} else {
			return 0;
		}
	}

	public List<Integer> getReferencesAsList(int id) {
		OfInt references = getReferences(id);
		if (references == null) {
			return Collections.emptyList();
		} else {
			List<Integer> list = new ArrayList<>();
			while (references.hasNext()) {
				list.add(references.nextInt());
			}
			return list;
		}
	}

	public IntHashSet getReferencesAsPrimitiveSet(int id) {
		long index = entryIndex.getValue(id);
		if (index > 0) {
			int referencesCount = referenceBlockChain.getReferencesCount(index);
			IntHashSet set = new IntHashSet(referencesCount);
			OfInt references = referenceBlockChain.getReferences(index);
			while (references.hasNext()) {
				set.add(references.nextInt());
			}
			return set;
		} else {
			return new IntHashSet();
		}
	}

	public BitSet getReferencesAsBitSet(int id) {
		long index = entryIndex.getValue(id);
		if (index > 0) {
			BitSet bitSet = new BitSet();
			OfInt references = referenceBlockChain.getReferences(index);
			while (references.hasNext()) {
				bitSet.set(references.nextInt());
			}
			return bitSet;
		} else {
			return new BitSet();
		}
	}

	public void setReferenceEditValue(int id, MultiReferenceEditValue editValue) {
		if (!editValue.getSetReferences().isEmpty()) {
			List<Integer> references = RecordReference.createRecordIdsList(editValue.getSetReferences());
			setReferences(id, references);
		} else if (editValue.isRemoveAll()) {
			removeAllReferences(id);
			if (!editValue.getAddReferences().isEmpty()) {
				List<Integer> references = RecordReference.createRecordIdsList(editValue.getAddReferences());
				addReferences(id, references, false);
			}
		} else {
			if (!editValue.getRemoveReferences().isEmpty()) {
				List<Integer> references = RecordReference.createRecordIdsList(editValue.getRemoveReferences());
				removeReferences(id, references, false);
			}
			if (!editValue.getAddReferences().isEmpty()) {
				List<Integer> references = RecordReference.createRecordIdsList(editValue.getAddReferences());
				addReferences(id, references, false);
			}
		}
	}

	public void setReferences(int id, List<Integer> references) {
		long index = entryIndex.getValue(id);
		long newIndex = referenceBlockChain.create(references);
		entryIndex.setValue(id, newIndex);
		if (cyclicReferences) {
			if (index > 0) {
				List<Integer> addIds = new ArrayList<>();
				List<Integer> removeIds = new ArrayList<>();
				calculateChangedIds(index, references, addIds, removeIds);
				addCyclicReferences(id, addIds);
				removeCyclicReferences(id, removeIds);
			} else {
				addCyclicReferences(id, references);
			}
		}
		if (index > 0) {
			referenceBlockChain.removeAll(index);
		}
	}

	public void addReferences(int id, List<Integer> references) {
		addReferences(id, references, false);
	}

	public void addReferences(int id, List<Integer> references, boolean cyclic) {
		long index = entryIndex.getValue(id);
		if (index > 0) {
			OfInt iterator = referenceBlockChain.getReferences(index);
			Set<Integer> addSet = new HashSet<>();
			while (iterator.hasNext()) {
				int reference = iterator.nextInt();
				if (addSet.contains(reference)) {
					references.remove(reference);
					addSet.add(reference);
				}
			}

			index = referenceBlockChain.add(references, index);
		} else {
			index = referenceBlockChain.create(references);
		}
		entryIndex.setValue(id, index);
		if (cyclicReferences && !cyclic) {
			addCyclicReferences(id, references);
		}
	}

	public void removeReferences(int id, List<Integer> references) {
		removeReferences(id, references, false);
	}

	public void removeReferences(int id, List<Integer> references, boolean cyclic) {
		long index = entryIndex.getValue(id);
		if (index > 0) {
			index = referenceBlockChain.remove(new HashSet<>(references), index);
			entryIndex.setValue(id, index);
		}
		if (cyclicReferences && !cyclic) {
			removeCyclicReferences(id, references);
		}
	}

	public void removeAllReferences(int id) {
		long index = entryIndex.getValue(id);
		if (index > 0) {
			if (cyclicReferences) {
				OfInt oldReferences = referenceBlockChain.getReferences(index);
				List<Integer> removeReferences = new ArrayList<>();
				while (oldReferences.hasNext()) {
					removeReferences.add(oldReferences.next());
				}
				removeCyclicReferences(id, removeReferences);
			}
			referenceBlockChain.removeAll(index);
		}
	}

	private void addCyclicReferences(int id, List<Integer> references) {
		if (reverseSingleIndex != null) {
			for (Integer reference : references) {
				int previousValue = reverseSingleIndex.getValue(reference);
				if (previousValue > 0 && previousValue != id) {
					removeReferences(previousValue, Collections.singletonList(reference), true);
				}
				reverseSingleIndex.setIndexValue(reference, id);
			}
		} else {
			for (Integer reference : references) {
				reverseMultiIndex.addReferences(reference, Collections.singletonList(id), true);
			}
		}
	}

	private void removeCyclicReferences(int id, List<Integer> references) {
		if (reverseSingleIndex != null) {
			for (Integer reference : references) {
				int previousValue = reverseSingleIndex.getValue(reference);
				if (id == previousValue) {
					reverseSingleIndex.setIndexValue(reference, 0);
				}
			}
		} else {
			for (Integer reference : references) {
				reverseMultiIndex.removeReferences(reference, Collections.singletonList(id), true);
			}
		}
	}

	private void calculateChangedIds(long index, List<Integer> references, List<Integer> addIds, List<Integer> removeIds) {
		Set<Integer> referenceSet = new HashSet<>(references);
		Set<Integer> ignoreIdSet = new HashSet<>();
		OfInt oldReferences = referenceBlockChain.getReferences(index);
		while (oldReferences.hasNext()) {
			Integer recordId = oldReferences.next();
			if (referenceSet.contains(recordId)) {
				ignoreIdSet.add(recordId);
			} else {
				removeIds.add(recordId);
			}
		}
		for (Integer reference : references) {
			if (!ignoreIdSet.contains(reference)) {
				addIds.add(reference);
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

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale) {
		int order = ascending ? 1 : -1;
		sortEntries.sort((o1, o2) -> {
			int value1 = getReferencesCount(o1.getLeafId());
			int value2 = getReferencesCount(o2.getLeafId());
			return Integer.compare(value1, value2) * order;
		});
		return sortEntries;
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
		entryIndex.close();
		referenceBlockChain.flush();
	}

	@Override
	public void drop() {
		entryIndex.drop();
		referenceBlockChain.drop();
	}

	public BitSet filterEquals(BitSet bitSet, Set<Integer> compareIds) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = entryIndex.getValue(id);
			if (index > 0) {
				int referencesCount = referenceBlockChain.getReferencesCount(index);
				if (referencesCount == compareIds.size()) {
					OfInt references = referenceBlockChain.getReferences(index);
					boolean containsDifference = false;
					while (references.hasNext()) {
						if (!compareIds.contains(references.nextInt())) {
							containsDifference = true;
							break;
						}
					}
					if (!containsDifference) {
						result.set(id);
					}
				}
			}
		}
		return result;
	}

	public BitSet filterNotEquals(BitSet bitSet, Set<Integer> compareIds) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = entryIndex.getValue(id);
			if (index <= 0) {
				result.set(id);
			} else {
				int referencesCount = referenceBlockChain.getReferencesCount(index);
				if (referencesCount != compareIds.size()) {
					result.set(id);
				} else {
					OfInt references = referenceBlockChain.getReferences(index);
					boolean containsDifference = false;
					while (references.hasNext()) {
						if (!compareIds.contains(references.nextInt())) {
							containsDifference = true;
							break;
						}
					}
					if (containsDifference) {
						result.set(id);
					}
				}
			}
		}
		return result;
	}

	public BitSet filterIsEmpty(BitSet bitSet) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = entryIndex.getValue(id);
			if (index <= 0) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterIsNotEmpty(BitSet bitSet) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = entryIndex.getValue(id);
			if (index > 0) {
				result.set(id);
			}
		}
		return result;
	}

	private BitSet filterContainsAny(BitSet bitSet, Set<Integer> compareIds) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = entryIndex.getValue(id);
			if (index > 0) {
				OfInt references = referenceBlockChain.getReferences(index);
				while (references.hasNext()) {
					if (compareIds.contains(references.nextInt())) {
						result.set(id);
						break;
					}
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
			long index = entryIndex.getValue(id);
			if (index > 0) {
				BitSet referencesAsBitSet = getReferencesAsBitSet(id);
				referencesAsBitSet.and(compareSet);
				if (referencesAsBitSet.equals(compareSet)) {
					result.set(id);
				}
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
			long index = entryIndex.getValue(id);
			if (index > 0) {
				int referencesCount = referenceBlockChain.getReferencesCount(index);
				if (referencesCount == count) {
					result.set(id);
				}
			}
		}
		return result;
	}

	public BitSet filterEntryCountGreater(BitSet bitSet, int count) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = entryIndex.getValue(id);
			if (index > 0) {
				int referencesCount = referenceBlockChain.getReferencesCount(index);
				if (referencesCount > count) {
					result.set(id);
				}
			}
		}
		return result;
	}

	public BitSet filterEntryCountSmaller(BitSet bitSet, int count) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = entryIndex.getValue(id);
			if (index > 0) {
				int referencesCount = referenceBlockChain.getReferencesCount(index);
				if (referencesCount < count) {
					result.set(id);
				}
			} else {
				result.set(id);
			}
		}
		return result;
	}

}
