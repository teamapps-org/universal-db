/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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

import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.transaction.DataType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class TextIndex extends AbstractIndex<String, TextFilter> {

	private final LongIndex positionIndex;
	private final CharIndex charIndex;
	private final TextSearchIndex searchIndex;
	private final CollectionTextSearchIndex collectionSearchIndex;

	public TextIndex(String name, TableIndex table, CollectionTextSearchIndex collectionSearchIndex) {
		super(name, table, FullTextIndexingOptions.INDEXED);
		this.positionIndex = new LongIndex(name, table);
		this.charIndex = table.getCollectionCharIndex();
		this.searchIndex = null;
		this.collectionSearchIndex = collectionSearchIndex;
	}

	public TextIndex(String name, TableIndex table, boolean withLocalSearchIndex) {
		super(name, table, withLocalSearchIndex ? FullTextIndexingOptions.INDEXED : FullTextIndexingOptions.NOT_INDEXED);
		this.positionIndex = new LongIndex(name, table);
		this.charIndex = table.getCollectionCharIndex();
		if (withLocalSearchIndex) {
			searchIndex = new TextSearchIndex(getPath(), name);
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
	public void setGenericValue(int id, String value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, null);
	}

	public String getValue(int id) {
		long index = positionIndex.getValue(id);
		if (index == 0) {
			return null;
		}
		return charIndex.getText(index);
	}

	public void setValue(int id, String value) {
		boolean update = false;
		if (searchIndex != null && positionIndex.getValue(id) > 0) {
			update = true;
		}
		long index = positionIndex.getValue(id);
		if (index != 0) {
			charIndex.removeText(index);
		}
		if (value != null && !value.isEmpty()) {
			index = charIndex.setText(value);
			positionIndex.setValue(id, index);
		} else {
			positionIndex.setValue(id, 0);
		}
		if (searchIndex != null) {
			if (!update && (value == null || value.isEmpty())) {
				return;
			} else {
				String textValue = value == null ? "" : value;
				searchIndex.addValue(id, textValue, update);
			}
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
	public BitSet filter(BitSet records, TextFilter textFilter) {
		return filter(records, textFilter, true);
	}

	@Override
	public void close() {
		if (searchIndex != null) {
			searchIndex.commit(true);
		}
		positionIndex.close();
		charIndex.close();
	}

	@Override
	public void drop() {
		if (searchIndex != null) {
			searchIndex.drop();
		}
		positionIndex.drop();
		charIndex.drop();
	}

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending) {
		int order = ascending ? 1 : -1;
		sortEntries.sort((o1, o2) -> {
			String value1 = getValue(o1.getLeafId());
			String value2 = getValue(o2.getLeafId());
			if (value1 == null || value2 == null) {
				if (value1 == null && value2 == null) {
					return 0;
				} else if (value1 == null) {
					return -1 * order;
				} else {
					return order;
				}
			}
			return value1.compareToIgnoreCase(value2);
		});
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
			long index = positionIndex.getValue(id);
			if (index == 0) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterNotEmpty(BitSet bitSet) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = positionIndex.getValue(id);
			if (index != 0) {
				result.set(id);
			}
		}
		return result;
	}

	public BitSet filterLengthGreater(BitSet bitSet, int length) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = positionIndex.getValue(id);
			if (index > 0) {
				int textByteLength = charIndex.getTextByteLength(index);
				if (textByteLength > length) {
					result.set(id);
				}
			}
		}
		return result;
	}

	public BitSet filterLengthSmaller(BitSet bitSet, int length) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			long index = positionIndex.getValue(id);
			if (index > 0) {
				int textByteLength = charIndex.getTextByteLength(index);
				if (textByteLength < length) {
					result.set(id);
				}
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
