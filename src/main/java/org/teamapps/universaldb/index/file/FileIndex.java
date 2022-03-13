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
package org.teamapps.universaldb.index.file;

import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.binary.BinaryIndex;
import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.index.text.CollectionTextSearchIndex;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.transaction.DataType;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class FileIndex extends AbstractIndex<FileValue, FileFilter> {

	private final TextIndex uuidIndex;
	private final TextIndex hashIndex;
	private final LongIndex sizeIndex;
	private final BinaryIndex metaDataIndex;
	private final FullTextIndexingOptions fullTextIndexingOptions;
	private final CollectionTextSearchIndex fileDataIndex;
	private final FileStore fileStore;
	private final String filePath;

	public FileIndex(String name, TableIndex table, ColumnType columnType, FullTextIndexingOptions fullTextIndexingOptions, CollectionTextSearchIndex collectionSearchIndex, FileStore fileStore) {
		super(name, table, columnType, fullTextIndexingOptions);
		this.uuidIndex = new TextIndex(name + "-file-uuid", table, columnType, false);
		this.hashIndex = new TextIndex(name + "-file-hash", table, columnType, false);
		this.sizeIndex = new LongIndex(name + "-file-size", table, columnType);
		this.fileDataIndex = fullTextIndexingOptions.isIndex() ? new CollectionTextSearchIndex(getFullTextIndexPath(), name) : null;
		this.metaDataIndex = fullTextIndexingOptions.isIndex() ? new BinaryIndex(name, table, true, columnType) : null;
		this.fullTextIndexingOptions = fullTextIndexingOptions;
		this.filePath = getFQN().replace('.', '/');
		this.fileStore = fileStore;
	}

	@Override
	public IndexType getType() {
		return IndexType.FILE;
	}

	@Override
	public FileValue getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public boolean isEmpty(int id) {
		return getValue(id) == null;
	}


	@Override
	public void setGenericValue(int id, FileValue value) {
		setValue(id, value);
	}

	public FileValue getValue(int id) {
		String uuid = uuidIndex.getValue(id);
		if (uuid != null) {
			String hash = hashIndex.getValue(id);
			long size = sizeIndex.getValue(id);
			FileValue fileValue = new FileValue(uuid, hash, size);
			if (fullTextIndexingOptions.isIndex()) {
				byte[] value = metaDataIndex.getValue(id);
				if (value != null) {
					try {
						FileMetaData metaData = new FileMetaData(value);
						fileValue.setMetaData(metaData);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			Supplier<File> fileSupplier = fileStore.getFileSupplier(filePath, uuid, hash);
			fileValue.setFileSupplier(fileSupplier);
			return fileValue;
		}
		return null;
	}

	public void setValue(int id, FileValue value) {
		String uuid = uuidIndex.getValue(id);
		if (value == null) {
			if (uuid != null) {
				uuidIndex.setValue(id, null);
				hashIndex.setValue(id, null);
				sizeIndex.setValue(id, 0);
				if (fileDataIndex != null) {
					fileDataIndex.setRecordValues(id, Collections.emptyList(), true);
					metaDataIndex.setValue(id, null);
				}
			}
		} else {
			boolean update = uuid != null;
			uuidIndex.setValue(id, value.getUuid());
			hashIndex.setValue(id, value.getHash());
			sizeIndex.setValue(id, value.getSize());
			if (fileDataIndex != null && value.getMetaData() != null) {
				fileDataIndex.setRecordValues(id, value.getMetaData().getFullTextIndexData(), update);
				metaDataIndex.setValue(id, value.getMetaData().getMetaDataBytes());
			}
		}
	}

	public FileValue storeFile(File file) {
		if (file == null) {
			return null;
		}
		return storeFile(file, file.getName());
	}

	public FileValue storeFile(File file, String fileName) {
		if (file == null) {
			return null;
		}
		FileValue fileValue = new FileValue(file, fileName);
		return storeFile(fileValue);
	}

	public FileValue storeFile(FileValue fileValue) {
		if (fileValue == null || fileValue.retrieveFile() == null) {
			return null;
		}
		if (fullTextIndexingOptions.isIndex()) {
			FileMetaData metaData = FileUtil.parseFileMetaData(fileValue.retrieveFile(), fileValue.getMetaData());
			fileValue.setMetaData(metaData);
		}
		fileStore.setFile(filePath, fileValue.getUuid(), fileValue.getHash(), fileValue.retrieveFile());
		fileValue.setFileSupplier(fileStore.getFileSupplier(filePath, fileValue.getUuid(), fileValue.getHash()));
		return fileValue;
	}

	public void removeStoredFile(int id) {
		String uuid = uuidIndex.getValue(id);
		if (uuid != null) {
			fileStore.removeFile(filePath, uuid);
		}
	}

	@Override
	public void removeValue(int id) {
		setValue(id, null);
	}

	@Override
	public void writeTransactionValue(FileValue value, DataOutputStream dataOutputStream) throws IOException {
		dataOutputStream.writeInt(getMappingId());
		dataOutputStream.writeByte(DataType.FILE_VALUE.getId());
		value.writeValues(dataOutputStream);
	}

	@Override
	public FileValue readTransactionValue(DataInputStream dataInputStream) throws IOException {
		return new FileValue(dataInputStream);
	}

	@Override
	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext) {
		return sortEntries;
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			String uuid = uuidIndex.getValue(id);
			if (uuid != null) {
				String hash = hashIndex.getValue(id);
				long size = sizeIndex.getValue(id);
				byte[] metaData = metaDataIndex.getValue(id);
				dataOutputStream.writeInt(id);
				DataStreamUtil.writeStringWithLengthHeader(dataOutputStream,uuid);
				DataStreamUtil.writeStringWithLengthHeader(dataOutputStream,hash);
				dataOutputStream.writeLong(size);
				DataStreamUtil.writeByteArrayWithLengthHeader(dataOutputStream,metaData);
			}
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			String uuid = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
			String hash = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
			long size = dataInputStream.readLong();
			byte[] metaData = DataStreamUtil.readByteArrayWithLengthHeader(dataInputStream);
			uuidIndex.setValue(id, uuid);
			hashIndex.setValue(id, hash);
			sizeIndex.setValue(id, size);
			metaDataIndex.setValue(id, metaData);
		} catch (EOFException ignore) {}
	}

	@Override
	public BitSet filter(BitSet records, FileFilter fileFilter) {
		switch (fileFilter.getFilterType()) {
			case FULL_TEXT_FILTER:
				return filterFullText(records, fileFilter);
			case SIZE_EQUALS:
				return sizeIndex.filterEquals(records, fileFilter.getSize());
			case SIZE_NOT_EQUALS:
				return sizeIndex.filterNotEquals(records, fileFilter.getSize());
			case SIZE_GREATER:
				return sizeIndex.filterGreater(records, fileFilter.getSize());
			case SIZE_SMALLER:
				return sizeIndex.filterSmaller(records, fileFilter.getSize());
			case SIZE_BETWEEN:
				return sizeIndex.filterBetween(records, fileFilter.getSize(), fileFilter.getSize2());
		}
		return null;
	}

	public BitSet filterFullText(BitSet records, FileFilter fileFilter) {
		if (fileDataIndex != null) {
			return fileDataIndex.filter(records, fileFilter.getTextFilters(), false);
		} else {
			return new BitSet();
		}
	}

	@Override
	public void close() {
		if (fileDataIndex != null) {
			fileDataIndex.commit(true);
		}
		uuidIndex.close();
		hashIndex.close();
		sizeIndex.close();
	}

	@Override
	public void drop() {
		if (fileDataIndex != null) {
			fileDataIndex.drop();
		}
		uuidIndex.drop();
		hashIndex.drop();
		sizeIndex.drop();
	}
}
