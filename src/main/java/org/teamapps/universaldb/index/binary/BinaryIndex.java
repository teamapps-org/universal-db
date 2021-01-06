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
package org.teamapps.universaldb.index.binary;

import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.transaction.DataType;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

public class BinaryIndex extends AbstractIndex<byte[], BinaryFilter> {

	private final LongIndex positionIndex;
	private final ByteArrayIndex byteArrayIndex;
	private final boolean compressContent;

	public BinaryIndex(String name, TableIndex table, boolean compressContent, ColumnType columnType) {
		super(name, table, columnType, FullTextIndexingOptions.NOT_INDEXED);
		this.compressContent = compressContent;
		positionIndex = new LongIndex(name, table, columnType);
		byteArrayIndex = new ByteArrayIndex(getPath(), name, this.compressContent);
	}

	@Override
	public IndexType getType() {
		return IndexType.BINARY;
	}

	@Override
	public byte[] getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public void setGenericValue(int id, byte[] value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, null);
	}

	public int getLength(int id) {
		long index = positionIndex.getValue(id);
		if (index > 0) {
			return byteArrayIndex.getByteArrayLength(index);
		} else {
			return 0;
		}
	}

	public Supplier<InputStream> getInputStreamSupplier(int id) {
		return () -> {
			byte[] value = getValue(id);
			if (value == null) {
				return null;
			} else {
				return new ByteArrayInputStream(value);
			}
		};
	}

	public byte[] getValue(int id) {
		long index = positionIndex.getValue(id);
		if (index == 0) {
			return null;
		}
		return byteArrayIndex.getByteArray(index);
	}

	public void setValue(int id, byte[] value) {
		long index = positionIndex.getValue(id);
		if (index != 0) {
			byteArrayIndex.removeByteArray(index);
		}
		if (value != null && value.length > 0) {
			index = byteArrayIndex.setByteArray(value);
			positionIndex.setValue(id, index);
		} else {
			positionIndex.setValue(id, 0);
		}
	}

	@Override
	public void writeTransactionValue(byte[] value, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.STRING.getId());
		dataOutputStream.writeInt(value.length);
		dataOutputStream.write(value);
	}

	@Override
	public byte[] readTransactionValue(DataInputStream dataInputStream) throws IOException {
		int length = dataInputStream.readInt();
		byte[] bytes = new byte[length];
		dataInputStream.read(bytes);
		return bytes;
	}

	@Override
	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, Locale locale) {
		return sortEntries;
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			byte[] value = getValue(id);
			dataOutputStream.writeInt(id);
			DataStreamUtil.writeByteArrayWithLengthHeader(dataOutputStream,value);
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			byte[] value = DataStreamUtil.readByteArrayWithLengthHeader(dataInputStream);
			setValue(id, value);
		} catch (EOFException ignore) {}
	}

	@Override
	public BitSet filter(BitSet records, BinaryFilter binaryFilter) {
		return null;
	}

	@Override
	public void close() {
		positionIndex.close();
		byteArrayIndex.close();
	}

	@Override
	public void drop() {
		positionIndex.drop();
		byteArrayIndex.drop();
	}
}
