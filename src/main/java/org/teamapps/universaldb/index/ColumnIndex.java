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
package org.teamapps.universaldb.index;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.binary.BinaryIndex;
import org.teamapps.universaldb.index.bool.BooleanIndex;
import org.teamapps.universaldb.index.file.FileIndex;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.numeric.*;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.reference.value.ReferenceIteratorValue;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.index.translation.TranslatableTextIndex;
import org.teamapps.universaldb.query.IndexFilter;
import org.teamapps.universaldb.query.IndexPath;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.PrimitiveIterator;
import java.util.stream.Collectors;

public interface ColumnIndex<TYPE, FILTER> extends MappedObject {

	static ColumnIndex createColumn(TableIndex table, String name, ColumnType columnType) {
		ColumnIndex column = null;

		switch (columnType.getIndexType()) {
			case BOOLEAN:
				column = new BooleanIndex(name, table, columnType);
				break;
			case SHORT:
				column = new ShortIndex(name, table, columnType);
				break;
			case INT:
				column = new IntegerIndex(name, table, columnType);
				break;
			case LONG:
				column = new LongIndex(name, table, columnType);
				break;
			case FLOAT:
				column = new FloatIndex(name, table, columnType);
				break;
			case DOUBLE:
				column = new DoubleIndex(name, table, columnType);
				break;
			case TEXT:
				column = new TextIndex(name, table, columnType, table.getCollectionTextSearchIndex());
				break;
			case TRANSLATABLE_TEXT:
				column = new TranslatableTextIndex(name, table, columnType, table.getCollectionTextSearchIndex());
				break;
			case REFERENCE:
				column = new SingleReferenceIndex(name, table, columnType);
				break;
			case MULTI_REFERENCE:
				column = new MultiReferenceIndex(name, table, columnType);
				break;
			case FILE:
				column = new FileIndex(name, table, columnType, FullTextIndexingOptions.INDEXED, table.getCollectionTextSearchIndex(), table.getFileStore());
				break;
			case BINARY:
				column = new BinaryIndex(name, table, false, columnType);
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
					return "(" + referenceIteratorValue.getAsList().stream().limit(100).map(v -> "" + v).collect(Collectors.joining(", ")) + ")";
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

	ColumnType getColumnType();

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

	List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext);

	BitSet filter(BitSet records, FILTER filter);

	default void dumpIndex(File file, BitSet records) throws IOException {
		DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file), 128_000));
		dumpIndex(dataOutputStream, records);
		dataOutputStream.close();
	}

	void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException;

	default void restoreIndex(File file) throws IOException{
		DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(file), 128_000));
		restoreIndex(dataInputStream);
		dataInputStream.close();
	}

	void restoreIndex(DataInputStream dataInputStream) throws IOException;

	void close();

	void drop();

}
