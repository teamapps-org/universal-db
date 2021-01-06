/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
package org.teamapps.universaldb.index.translation;

import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.index.text.*;
import org.teamapps.universaldb.transaction.DataType;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.*;

public class TranslatableTextIndex extends AbstractIndex<TranslatableText, TranslatableTextFilter> {

	private final LongIndex positionIndex;
	private final CharIndex charIndex;
	private final TextSearchIndex searchIndex;
	private final CollectionTextSearchIndex collectionSearchIndex;

	public TranslatableTextIndex(String name, TableIndex table, ColumnType columnType, CollectionTextSearchIndex collectionSearchIndex) {
		super(name, table, columnType, FullTextIndexingOptions.INDEXED);
		this.positionIndex = new LongIndex(name, table, columnType);
		this.charIndex = table.getCollectionCharIndex();
		this.searchIndex = null;
		this.collectionSearchIndex = collectionSearchIndex;
	}

	public TranslatableTextIndex(String name, TableIndex table, ColumnType columnType, boolean withLocalSearchIndex) {
		super(name, table, columnType, withLocalSearchIndex ? FullTextIndexingOptions.INDEXED : FullTextIndexingOptions.NOT_INDEXED);
		this.positionIndex = new LongIndex(name, table, columnType);
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

	public boolean isFilteredByCollectionTextIndex(TranslatableTextFilter filter) {
		return collectionSearchIndex != null && filter.getFilterType().containsFullTextPart();
	}

	public boolean isFilteredExclusivelyByCollectionTextIndex(TranslatableTextFilter filter) {
		return collectionSearchIndex != null && filter.getFilterType().isFullTextIndexExclusive();
	}

	@Override
	public IndexType getType() {
		return IndexType.TRANSLATABLE_TEXT;
	}

	@Override
	public TranslatableText getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public void setGenericValue(int id, TranslatableText value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, null);
	}

	public String getTranslatedValue(int id, String language) {
		TranslatableText value = getValue(id);
		return value != null ? value.translationLookup(language) : null;
	}

	public String getTranslatedValue(int id, List<String> languages) {
		TranslatableText value = getValue(id);
		return value != null ? value.getTranslation(languages) : null;
	}

	public TranslatableText getValue(int id) {
		long index = positionIndex.getValue(id);
		if (index == 0) {
			return null;
		}
		return new TranslatableText(charIndex.getText(index));
	}

	public void setValue(int id, TranslatableText value) {
		boolean update = false;
		if (searchIndex != null && positionIndex.getValue(id) > 0) {
			update = true;
		}
		long index = positionIndex.getValue(id);
		if (index != 0) {
			charIndex.removeText(index);
		}
		String encodedValue = value != null ? value.getEncodedValue() : null;
		if (encodedValue != null) {
			index = charIndex.setText(encodedValue);
			positionIndex.setValue(id, index);
		} else {
			positionIndex.setValue(id, 0);
		}
		if (searchIndex != null) {
			if (!update && encodedValue == null) {
				return;
			} else {
				searchIndex.addValue(id, value, update);
			}
		}
	}

	@Override
	public void writeTransactionValue(TranslatableText value, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.STRING.getId());
		value.writeValues(dataOutputStream);
	}

	@Override
	public TranslatableText readTransactionValue(DataInputStream dataInputStream) throws IOException {
		return new TranslatableText(dataInputStream);
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			TranslatableText value = getValue(id);
			dataOutputStream.writeInt(id);
			DataStreamUtil.writeTranslatableText(dataOutputStream, value);
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			TranslatableText value = DataStreamUtil.readTranslatableText(dataInputStream);
			setValue(id, value);
		} catch (EOFException ignore) {}
	}

	@Override
	public BitSet filter(BitSet records, TranslatableTextFilter textFilter) {
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

	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale) { //todo add locale/language to sorting
		int order = ascending ? 1 : -1;
		String language = locale.getLanguage();

		sortEntries.sort((o1, o2) -> {
			String value1 = getTranslatedValue(o1.getLeafId(), language);
			String value2 = getTranslatedValue(o2.getLeafId(), language);
			if (value1 == null || value2 == null) {
				if (value1 == null && value2 == null) {
					return 0;
				} else if (value1 == null) {
					return -1 * order;
				} else {
					return order;
				}
			}
			return value1.compareToIgnoreCase(value2) * order;
		});
		return sortEntries;
	}

	public BitSet filter(BitSet records, TranslatableTextFilter translatableTextFilter, boolean performLocalFullTextSearch) {
		BitSet fullTextResult = records;
		if (performLocalFullTextSearch) {
			if (translatableTextFilter.getFilterType().containsFullTextPart()) {
				if (searchIndex != null) {
					fullTextResult = searchIndex.filter(records, translatableTextFilter);
				} else if (collectionSearchIndex != null) {
					fullTextResult = collectionSearchIndex.filter(records, Collections.emptyList(), Collections.singletonList(TranslatableTextFieldFilter.create(translatableTextFilter, getName())), true);
				} else {
					return null;
				}
				if (!translatableTextFilter.getFilterType().containsIndexPart()) {
					return fullTextResult;
				}
			}
		}

		if (translatableTextFilter.getFilterType().containsIndexPart()) {
			switch (translatableTextFilter.getFilterType()) {
				case EMPTY:
					return filterEmpty(records);
				case NOT_EMPTY:
					return filterNotEmpty(records);
				case TEXT_EQUALS:
					return filterEquals(fullTextResult, translatableTextFilter.getValue(), translatableTextFilter.getLanguage());
				case TEXT_NOT_EQUALS:
					return filterNotEquals(fullTextResult, translatableTextFilter.getValue(), translatableTextFilter.getLanguage());
				case TEXT_BYTE_LENGTH_GREATER:
					return filterLengthGreater(records, Integer.parseInt(translatableTextFilter.getValue()));
				case TEXT_BYTE_LENGTH_SMALLER:
					return filterLengthSmaller(records, Integer.parseInt(translatableTextFilter.getValue()));
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


	private BitSet filterEquals(BitSet bitSet, String value, String language) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			TranslatableText translatableText = getValue(id);
			if (Objects.equals(translatableText.translationLookup(language), value)) {
				result.set(id);
			}
		}
		return result;
	}

	private BitSet filterNotEquals(BitSet bitSet, String value, String language) {
		BitSet result = new BitSet();
		for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
			TranslatableText translatableText = getValue(id);
			if (!Objects.equals(translatableText.translationLookup(language), value)) {
				result.set(id);
			}
		}
		return result;
	}
}
