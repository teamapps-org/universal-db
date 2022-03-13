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
package org.teamapps.universaldb.index.text;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.buffer.BlockEntryAtomicStore;
import org.teamapps.universaldb.transaction.DataType;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TextIndex extends AbstractIndex<String, TextFilter> {

	private BlockEntryAtomicStore atomicStore;
	private final TextSearchIndex searchIndex;
	private final CollectionTextSearchIndex collectionSearchIndex;

	public TextIndex(String name, TableIndex table, ColumnType columnType, CollectionTextSearchIndex collectionSearchIndex) {
		super(name, table, columnType, FullTextIndexingOptions.INDEXED);
		atomicStore = new BlockEntryAtomicStore(table.getDataPath(), name);
		this.searchIndex = null;
		this.collectionSearchIndex = collectionSearchIndex;
	}

	public TextIndex(String name, TableIndex table, ColumnType columnType, boolean withLocalSearchIndex) {
		super(name, table, columnType, withLocalSearchIndex ? FullTextIndexingOptions.INDEXED : FullTextIndexingOptions.NOT_INDEXED);
		atomicStore = new BlockEntryAtomicStore(table.getDataPath(), name);
		if (withLocalSearchIndex) {
			searchIndex = new TextSearchIndex(getFullTextIndexPath(), name);
		} else {
			searchIndex = null;
		}
		collectionSearchIndex = null;
	}

	public CollectionTextSearchIndex getCollectionSearchIndex() {
		return collectionSearchIndex;
	}

	public boolean isFilteredByCollectionTextIndex(TextFilter filter) {
		return collectionSearchIndex != null && filter.getFilterType().containsFullTextPart();
	}

	public boolean isFilteredExclusivelyByCollectionTextIndex(TextFilter filter) {
		return collectionSearchIndex != null && filter.getFilterType().isFullTextIndexExclusive();
	}

	@Override
	public IndexType getType() {
		return IndexType.TEXT;
	}

	@Override
	public String getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public boolean isEmpty(int id) {
		return getValue(id) == null;
	}

	@Override
	public void setGenericValue(int id, String value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, null);
	}

	public String getValue(int id) {
		return atomicStore.getText(id);
	}

	public void setValue(int id, String value) {
		boolean update = !atomicStore.isEmpty(id);
		atomicStore.setText(id, value);
		if (searchIndex != null) {
			String textValue = value == null ? "" : value;
			searchIndex.addValue(id, textValue, update);
		}
	}

	@Override
	public void writeTransactionValue(String value, DataOutputStream dataOutputStream) throws IOException {
		byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.STRING.getId());
		dataOutputStream.writeInt(bytes.length);
		dataOutputStream.write(bytes);
	}

	@Override
	public String readTransactionValue(DataInputStream dataInputStream) throws IOException {
		int length = dataInputStream.readInt();
		byte[] bytes = new byte[length];
		dataInputStream.read(bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			String value = getValue(id);
			dataOutputStream.writeInt(id);
			DataStreamUtil.writeStringWithLengthHeader(dataOutputStream, value);
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			String value = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
			setValue(id, value);
		} catch (EOFException ignore) {}
	}

	@Override
	public BitSet filter(BitSet records, TextFilter textFilter) {
		return filter(records, textFilter, true);
	}

	@Override
	public void close() {
		if (searchIndex != null) {
			searchIndex.commit(true);
		}
		atomicStore.close();
	}

	@Override
	public void drop() {
		if (searchIndex != null) {
			searchIndex.drop();
		}
		atomicStore.drop();
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext) {
		Comparator<String> comparator = UserContext.getOrCreateComparator(userContext, ascending);
		sortEntries.sort((o1, o2) -> comparator.compare(getValue(o1.getLeafId()), getValue(o2.getLeafId())));
		return sortEntries;
	}

	public BitSet filter(BitSet records, TextFilter textFilter, boolean performLocalFullTextSearch) {
		BitSet fullTextResult = records;
		if (performLocalFullTextSearch) {
			if (textFilter.getFilterType().containsFullTextPart()) {
				if (searchIndex != null) {
					fullTextResult = searchIndex.filter(records, textFilter);
				} else if (collectionSearchIndex != null) {
					fullTextResult = collectionSearchIndex.filter(records, Collections.singletonList(TextFieldFilter.create(textFilter, getName())), true);
				} else {
					return null;
				}
				if (!textFilter.getFilterType().containsIndexPart()) {
					return fullTextResult;
				}
			}
		}

		if (textFilter.getFilterType().containsIndexPart()) {
			switch (textFilter.getFilterType()) {
				case EMPTY:
					return filterEmpty(records);
				case NOT_EMPTY:
					return filterNotEmpty(records);
				case TEXT_EQUALS:
					return filterEquals(fullTextResult, textFilter.getValue());
				case TEXT_EQUALS_IGNORE_CASE:
					return filterEqualsIgnoreCase(fullTextResult, textFilter.getValue());
				case TEXT_NOT_EQUALS:
					return filterNotEquals(fullTextResult, textFilter.getValue());
				case TEXT_BYTE_LENGTH_GREATER:
					return filterLengthGreater(records, Integer.parseInt(textFilter.getValue()));
				case TEXT_BYTE_LENGTH_SMALLER:
					return filterLengthSmaller(records, Integer.parseInt(textFilter.getValue()));
				default:
					return null;
			}
		}
		return null;
	}

	public BitSet filterEmpty(BitSet bitSet) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			if (atomicStore.isEmpty(id)) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterNotEmpty(BitSet bitSet) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			if (!atomicStore.isEmpty(id)) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterLengthGreater(BitSet bitSet, int length) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int blockLength = atomicStore.getBlockLength(id);
			if (blockLength > length) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterLengthSmaller(BitSet bitSet, int length) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			int blockLength = atomicStore.getBlockLength(id);
			if (blockLength < length) {
				result.set(id);
			}
		}
		return result;
	}


	private BitSet filterEquals(BitSet bitSet, String value) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			String text = getValue(id);
			if (Objects.equals(text, value)) {
				result.set(id);
			}
		}
		return result;
	}

	private BitSet filterEqualsIgnoreCase(BitSet bitSet, String value) {
		BitSet result = new BitSet();
		if (value == null) {
			return result;
		}
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			String text = getValue(id);
			if (value.equalsIgnoreCase(text)) {
				result.set(id);
			}
		}
		return result;
	}

	private BitSet filterNotEquals(BitSet bitSet, String value) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			String text = getValue(id);
			if (!Objects.equals(text, value)) {
				result.set(id);
			}
		}
		return result;
	}
}
