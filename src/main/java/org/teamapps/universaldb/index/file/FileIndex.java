/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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

import org.teamapps.message.protocol.utils.MessageUtils;
import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.AbstractIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.SortEntry;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.buffer.index.ByteArrayAtomicMappedIndex;
import org.teamapps.universaldb.index.buffer.index.LongAtomicMappedIndex;
import org.teamapps.universaldb.index.buffer.index.StringAtomicMappedIndex;
import org.teamapps.universaldb.index.file.store.DatabaseFileStore;
import org.teamapps.universaldb.index.file.store.FileStoreUtil;
import org.teamapps.universaldb.index.file.value.*;
import org.teamapps.universaldb.index.text.CollectionTextSearchIndex;
import org.teamapps.universaldb.message.MessageStore;
import org.teamapps.universaldb.message.MessageStoreImpl;
import org.teamapps.universaldb.model.FileFieldModel;

import java.io.*;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class FileIndex extends AbstractIndex<FileValue, FileFilter> {

	private final StringAtomicMappedIndex nameIndex;
	private final LongAtomicMappedIndex sizeIndex;
	private final ByteArrayAtomicMappedIndex hashIndex;
	private final ByteArrayAtomicMappedIndex keyIndex;
	private final FileFieldModel fileFieldModel;
	private final DatabaseFileStore fileStore;
	private final boolean fileStoreEncrypted;
	private CollectionTextSearchIndex fullTextIndex;
	private MessageStore<FileContentData> contentDataMessageStore;

	public FileIndex(FileFieldModel fileFieldModel, TableIndex tableIndex) {
		super(fileFieldModel, tableIndex);
		this.fileFieldModel = fileFieldModel;
		this.fileStore = tableIndex.getDatabaseIndex().getDatabaseFileStore();
		nameIndex = new StringAtomicMappedIndex(tableIndex.getDataPath(), fileFieldModel.getName());
		sizeIndex = new LongAtomicMappedIndex(tableIndex.getDataPath(), fileFieldModel.getName() + "-len");
		hashIndex = new ByteArrayAtomicMappedIndex(tableIndex.getDataPath(), fileFieldModel.getName() + "-hash");
		fileStoreEncrypted = fileStore.isEncrypted();
		keyIndex = fileStoreEncrypted ? new ByteArrayAtomicMappedIndex(tableIndex.getDataPath(), fileFieldModel.getName() + "-hash") : null;
		if (fileFieldModel.isIndexContent()) {
			contentDataMessageStore = new MessageStoreImpl<>(tableIndex.getDataPath(), fileFieldModel.getName() + "-file-meta", FileContentData.getMessageDecoder());
			fullTextIndex = new CollectionTextSearchIndex(tableIndex.getFullTextIndexPath(), fileFieldModel.getName());
		}
	}

	public FileFieldModel getFileFieldModel() {
		return fileFieldModel;
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
		long size = sizeIndex.getValue(id);
		if (size != 0) {
			String name = nameIndex.getValue(id);
			String hash = FileStoreUtil.bytesToHex(hashIndex.getValue(id));
			String key = fileStoreEncrypted ? FileStoreUtil.bytesToHex(keyIndex.getValue(id)) : null;
			File file = fileStore.getLocalFile(hash, size, key);
			Supplier<FileContentData> contentDataSupplier = fileFieldModel.isIndexContent() ? () -> contentDataMessageStore.getById(id) : null;
			if (file != null) {
				return new CommittedLocalFile(file, name, hash, size, contentDataSupplier);
			} else {
				return new CommittedRemoteFile(() -> fileStore.loadRemoteFile(hash, size, key), name, hash, size, contentDataSupplier);
			}
		} else {
			return null;
		}
	}

	public void setValue(int id, FileValue value) {
		if (value != null && value.getType() == FileValueType.UNCOMMITTED_FILE) {
			throw new RuntimeException("Error saving uncommitted file is not possible!");
		}
		boolean update = sizeIndex.getValue(id) != 0;
		if (value == null || value.getSize() == 0) {
			if (update) {
				sizeIndex.setValue(id, 0);
				nameIndex.setValue(id, null);
				hashIndex.removeValue(id);
				if (fileFieldModel.isIndexContent()) {
					fullTextIndex.setRecordValues(id, Collections.emptyList(), true);
					contentDataMessageStore.delete(id);
				}
			}
		} else {
			sizeIndex.setValue(id, value.getSize());
			nameIndex.setValue(id, value.getFileName());
			hashIndex.setValue(id, value.getHashBytes());
			if (fileFieldModel.isIndexContent()) {
				FileContentData contentData = value.getFileContentData();
				contentData.setRecordId(id);
				fullTextIndex.setRecordValues(id, value.getFullTextIndexData(), update);
				contentDataMessageStore.save(contentData);
			}
		}
	}

	public FileValue storeFile(File file) {
		return (file != null && file.exists() && file.length() > 0) ? storeFile(file, file.getName()) : null;
	}

	public FileValue storeFile(File file, String fileName) {
		FileValue fileValue = FileValue.create(file, fileName);
		String key = fileStore.storeFile(file, fileValue.getHash(), fileValue.getSize());
		FileContentData contentData = fileFieldModel.isIndexContent() ? fileValue.getFileContentData(fileFieldModel.getMaxIndexContentLength()) : null;
		if (fileFieldModel.isIndexContent() && fileFieldModel.isDetectLanguage()) {
			fileValue.getDetectedLanguage();
		}
		return new StoreDescriptionFile(file, fileName, fileValue.getSize(), fileValue.getHash(), key, contentData);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, null);
	}

	@Override
	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext) {
		return sortEntries;
	}

	@Override
	public void dumpIndex(DataOutputStream dos, BitSet records) throws IOException {
		boolean withContent = fileFieldModel.isIndexContent();
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			long size = sizeIndex.getValue(id);
			if (size > 0) {
				dos.writeInt(id);
				MessageUtils.writeString(dos, nameIndex.getValue(id));
				MessageUtils.writeByteArray(dos, hashIndex.getValue(id));
				dos.writeLong(size);
				if (withContent) {
					FileContentData contentData = contentDataMessageStore.getById(id);
					if (contentData != null) {
						dos.writeBoolean(true);
						MessageUtils.writeByteArray(dos, contentData.toBytes());
					}
				}
			}
		}
	}

	@Override
	public void restoreIndex(DataInputStream dis) throws IOException {
		try {
			int id = dis.readInt();
			String name = MessageUtils.readString(dis);
			byte[] hash = MessageUtils.readByteArray(dis);
			long size = dis.readLong();
			nameIndex.setValue(id, name);
			hashIndex.setValue(id, hash);
			sizeIndex.setValue(id, size);
			if (dis.readBoolean()) {
				byte[] bytes = MessageUtils.readByteArray(dis);
				FileContentData contentData = new FileContentData(bytes);
				contentDataMessageStore.save(contentData);
			}
		} catch (EOFException ignore) {
		}
	}

	@Override
	public BitSet filter(BitSet records, FileFilter fileFilter) {
		return switch (fileFilter.getFilterType()) {
			case FULL_TEXT_FILTER -> filterFullText(records, fileFilter);
			case SIZE_EQUALS -> sizeIndex.filterEquals(fileFilter.getSize(), records);
			case SIZE_NOT_EQUALS -> sizeIndex.filterNotEquals(fileFilter.getSize(), records);
			case SIZE_GREATER -> sizeIndex.filterGreater(fileFilter.getSize(), records);
			case SIZE_SMALLER -> sizeIndex.filterSmaller(fileFilter.getSize(), records);
			case SIZE_BETWEEN -> sizeIndex.filterBetween(fileFilter.getSize(), fileFilter.getSize2(), records);
		};
	}

	public BitSet filterFullText(BitSet records, FileFilter fileFilter) {
		if (fileFieldModel.isIndexContent()) {
			return fullTextIndex.filter(records, fileFilter.getTextFilters(), false);
		} else {
			return new BitSet();
		}
	}

	@Override
	public void close() {
		if (fullTextIndex != null) {
			contentDataMessageStore.close();
			fullTextIndex.commit(true);
		}
		nameIndex.close();
		hashIndex.close();
		sizeIndex.close();
	}

	@Override
	public void drop() {
		if (fullTextIndex != null) {
			contentDataMessageStore.drop();
			fullTextIndex.drop();
		}
		nameIndex.drop();
		hashIndex.drop();
		sizeIndex.drop();
	}
}
