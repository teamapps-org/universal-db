package org.teamapps.universaldb.index.file2;

import org.teamapps.message.protocol.file.FileData;
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
import org.teamapps.universaldb.index.file.FileMetaData;
import org.teamapps.universaldb.index.file.FileUtil;
import org.teamapps.universaldb.index.text.CollectionTextSearchIndex;
import org.teamapps.universaldb.message.MessageStore;
import org.teamapps.universaldb.message.MessageStoreImpl;
import org.teamapps.universaldb.model.FileFieldModel;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class FileIndex2 extends AbstractIndex<FileValue, FileFilter> {

	private final StringAtomicMappedIndex nameIndex;
	private final LongAtomicMappedIndex sizeIndex;
	private final ByteArrayAtomicMappedIndex hashIndex;
	private final ByteArrayAtomicMappedIndex keyIndex;
	private final FileFieldModel fileFieldModel;
	private final FileStore fileStore;
	private CollectionTextSearchIndex fullTextIndex;
	private MessageStore<FileContentData> contentDataMessageStore;

	public FileIndex2(FileFieldModel fileFieldModel, TableIndex tableIndex, FileStore fileStore) {
		super(fileFieldModel, tableIndex);
		this.fileFieldModel = fileFieldModel;
		this.fileStore = fileStore;
		nameIndex = new StringAtomicMappedIndex(tableIndex.getDataPath(), fileFieldModel.getName());
		sizeIndex = new LongAtomicMappedIndex(tableIndex.getDataPath(), fileFieldModel.getName() + "-len");
		hashIndex = new ByteArrayAtomicMappedIndex(tableIndex.getDataPath(), fileFieldModel.getName() + "-hash");
		keyIndex = fileStore.isEncrypted() ? new ByteArrayAtomicMappedIndex(tableIndex.getDataPath(), fileFieldModel.getName() + "-hash") : null;
		if (fileFieldModel.isIndexContent()) {
			contentDataMessageStore = new MessageStoreImpl<>(tableIndex.getDataPath(), fileFieldModel.getName() + "-file-meta", FileContentData.getMessageDecoder());
		}

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
			return new FileValue(nameIndex.getValue(id), size, hashIndex.getValue(id), () -> contentDataMessageStore.getById(id), /* todo */ null);
		} else {
			return null;
		}
	}

	public void setValue(int id, FileValue value) {
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
				//todo remove file
			}
		} else {
			sizeIndex.setValue(id, value.getSize());
			nameIndex.setValue(id, value.getFileName());
			hashIndex.setValue(id, value.getHashBytes());

			if (fileFieldModel.isIndexContent()) {
				FileContentData contentData = value.getFileContentData(fileFieldModel.getMaxIndexContentLength());
				contentData.setRecordId(id);
				fullTextIndex.setRecordValues(id, value.getFullTextIndexData(fileFieldModel.getMaxIndexContentLength()), update);
				contentDataMessageStore.save(contentData);
			}
			//todo save file
		}

	}

//	public FileValue storeFile(File file) {
//		if (file == null) {
//			return null;
//		}
//		return storeFile(file, file.getName());
//	}
//
//	public FileValue storeFile(File file, String fileName) {
//		if (file == null) {
//			return null;
//		}
//		FileValue fileValue = new FileValue(file, fileName);
//		return storeFile(fileValue);
//	}
//
//	public FileValue storeFile(FileValue fileValue) {
//		if (fileValue == null || fileValue.retrieveFile() == null) {
//			return null;
//		}
//		if (fullTextIndexingOptions.isIndex()) {
//			FileMetaData metaData = FileUtil.parseFileMetaData(fileValue.retrieveFile(), fileValue.getMetaData());
//			fileValue.setMetaData(metaData);
//		}
//		fileStore.setFile(filePath, fileValue.getUuid(), fileValue.getHash(), fileValue.retrieveFile());
//		fileValue.setFileSupplier(fileStore.getFileSupplier(filePath, fileValue.getUuid(), fileValue.getHash()));
//		return fileValue;
//	}
//
//	public void removeStoredFile(int id) {
//		String uuid = uuidIndex.getValue(id);
//		if (uuid != null) {
//			fileStore.removeFile(filePath, uuid);
//		}
//	}

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
			fullTextIndex.commit(true);
		}
		nameIndex.close();
		hashIndex.close();
		sizeIndex.close();
	}

	@Override
	public void drop() {
		if (fullTextIndex != null) {
			fullTextIndex.drop();
		}
		nameIndex.drop();
		hashIndex.drop();
		sizeIndex.drop();
	}
}
