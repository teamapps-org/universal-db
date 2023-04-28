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
package org.teamapps.universaldb.index;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.reference.value.ReferenceIteratorValue;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.model.FieldModel;
import org.teamapps.universaldb.model.FieldType;
import org.teamapps.universaldb.query.IndexFilter;
import org.teamapps.universaldb.query.IndexPath;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

public interface FieldIndex<TYPE, FILTER> extends MappedObject {

	String getName();

	TableIndex getTable();

	FieldType getFieldType();

	FieldModel getFieldModel();

	String getFQN();

	IndexType getType();

	int getMappingId();

	TYPE getGenericValue(int id);

	boolean isEmpty(int id);

	void setGenericValue(int id, TYPE value);

	void removeValue(int id);

	default FieldIndex getReferencedColumn() {
		return null;
	}

	default IndexFilter<TYPE, FILTER> createFilter(FILTER filter) {
		return new IndexFilter<>(this, filter);
	}

	default IndexFilter<TYPE, FILTER> createFilter(FILTER filter, IndexPath indexPath) {
		return new IndexFilter<>(this, filter, indexPath);
	}

	List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext);

	BitSet filter(BitSet records, FILTER filter);

	default String getStringValue(int id) {
		Object value = getGenericValue(id);
		if (value == null) {
			return "NULL";
		} else {
			switch (getType()) {
				case TRANSLATABLE_TEXT:
					TranslatableText translatableText = (TranslatableText) value;
					return translatableText.getText();
				case MULTI_REFERENCE:
					ReferenceIteratorValue referenceIteratorValue = (ReferenceIteratorValue) value;
					return "(" + referenceIteratorValue.getAsList().stream().limit(100).map(String::valueOf).collect(Collectors.joining(", ")) + ")";
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
