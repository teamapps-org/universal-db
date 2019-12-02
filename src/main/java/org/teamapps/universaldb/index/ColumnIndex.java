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
package org.teamapps.universaldb.index;

import org.teamapps.universaldb.index.binary.BinaryIndex;
import org.teamapps.universaldb.index.translation.TranslatableTextIndex;
import org.teamapps.universaldb.util.DataStreamUtil;
import org.teamapps.universaldb.index.bool.BitSetBooleanIndex;
import org.teamapps.universaldb.index.file.FileIndex;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.numeric.*;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.reference.value.ReferenceIteratorValue;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.query.IndexFilter;
import org.teamapps.universaldb.query.IndexPath;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.PrimitiveIterator;

public interface ColumnIndex<TYPE, FILTER> extends MappedObject {

	static ColumnIndex createColumn(TableIndex table, String name, IndexType indexType) {
		ColumnIndex column = null;

		switch (indexType) {
			case BOOLEAN:
				//todo: bit set boolean vs. boolean index -> check sub type!
				column = new BitSetBooleanIndex(name, table);
				break;
			case SHORT:
				column = new ShortIndex(name, table);
				break;
			case INT:
				column = new IntegerIndex(name, table);
				break;
			case LONG:
				column = new LongIndex(name, table);
				break;
			case FLOAT:
				column = new FloatIndex(name, table);
				break;
			case DOUBLE:
				column = new DoubleIndex(name, table);
				break;
			case TEXT:
				column = new TextIndex(name, table, table.getCollectionTextSearchIndex());
				break;
			case TRANSLATABLE_TEXT:
				column = new TranslatableTextIndex(name, table, table.getCollectionTextSearchIndex());
				break;
			case REFERENCE:
				column = new SingleReferenceIndex(name, table);
				break;
			case MULTI_REFERENCE:
				column = new MultiReferenceIndex(name, table, table.getReferenceBlockChain());
				break;
			case FILE:
				column = new FileIndex(name, table, FullTextIndexingOptions.INDEXED, table.getCollectionTextSearchIndex(), table.getFileStore());
				break;
			case BINARY:
				column = new BinaryIndex(name, table, false);
				break;
		}
		return column;
	}

	default String getStringValue(int id) {
		Object value = getGenericValue(id);
		if (value == null) {
			return "NULL";
		} else {
			switch (getType()) {
				case MULTI_REFERENCE:
					ReferenceIteratorValue referenceIteratorValue = (ReferenceIteratorValue) value;
					PrimitiveIterator.OfInt idIterator = referenceIteratorValue.getIdIterator();
					StringBuilder sb = new StringBuilder();
					sb.append("(");
					int counter = 0;
					while (idIterator.hasNext()) {
						if (counter > 0) {
							sb.append(", ");
						}
						if (counter > 100) {
							sb.append("...");
							break;
						}
						sb.append(idIterator.nextInt());
						counter++;
					}
					sb.append(")");
					return sb.toString();
				case FILE:
					FileValue fileValue = (FileValue) value;
					if (fileValue.getMetaData() != null) {
						return fileValue.getMetaData().getName() + " (" + fileValue.getMetaData().getSize() + ")";
					} else {
						return fileValue.getUuid();
					}
				case BINARY:
					byte[] byteValue = (byte[]) value;
					return byteValue.length + " bytes";
			}
			return value.toString();
		}
	}

	String getName();

	TableIndex getTable();

	String getFQN();

	IndexType getType();

	FullTextIndexingOptions getFullTextIndexingOptions();

	int getMappingId();

	void setMappingId(int id);

	TYPE getGenericValue(int id);

	void setGenericValue(int id, TYPE value);

	void removeValue(int id);

	void writeTransactionValue(TYPE value, DataOutputStream dataOutputStream) throws IOException;

	TYPE readTransactionValue(DataInputStream dataInputStream) throws IOException;

	default ColumnIndex getReferencedColumn() {
		return null;
	}

	default IndexFilter<TYPE, FILTER> createFilter(FILTER filter) {
		return new IndexFilter<>(this, filter);
	}

	default IndexFilter<TYPE, FILTER> createFilter(FILTER filter, IndexPath indexPath) {
		return new IndexFilter<>(this, filter, indexPath);
	}

	default void writeSchema(DataOutputStream dataOutputStream) throws IOException {
		DataStreamUtil.writeStringWithLengthHeader(dataOutputStream, getName());
		dataOutputStream.writeInt(getType().getId());
		dataOutputStream.writeInt(getMappingId());
	}

	List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale);

	BitSet filter(BitSet records, FILTER filter);

	void close();

	void drop();

}
