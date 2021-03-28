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
package org.teamapps.universaldb.index.fileng;

import org.apache.commons.io.IOUtils;
import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.*;
import org.teamapps.universaldb.index.file.FileFilter;
import org.teamapps.universaldb.index.numeric.LongIndex;
import org.teamapps.universaldb.index.numeric.ShortIndex;
import org.teamapps.universaldb.index.text.CollectionTextSearchIndex;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class FileIndex extends AbstractIndex<FileValue, FileFilter> implements FileStore {

	private final TextIndex hashIndex;
	private final TextIndex nameIndex;
	private final LongIndex sizeIndex;
	private FileTextContentIndex textContentIndex;
	private ShortIndex versionIndex;
	private FileVersionDataIndex versionDataIndex;
	private final CollectionTextSearchIndex fullTextIndex;

	private final String filePath;
	private final boolean indexFileContent;
	private final boolean indexFileVersions;
	private final String secret;

	private final File localFileStorePath;
	private final RemoteFileStore remoteFileStore;

	public FileIndex(String name, TableIndex table, ColumnType columnType, boolean indexFileContent, boolean indexFileVersions, String secret) {
		super(name, table, columnType, indexFileContent ? FullTextIndexingOptions.INDEXED : FullTextIndexingOptions.NOT_INDEXED);
		this.indexFileContent = indexFileContent;
		this.indexFileVersions = indexFileVersions;
		this.secret = secret;
		hashIndex = new TextIndex(name + "-file-hash", table, columnType, false);
		nameIndex = new TextIndex(name + "-file-name", table, columnType, false);
		sizeIndex = new LongIndex(name + "-file-size", table, columnType);
		if (indexFileContent) {
			textContentIndex = new FileTextContentIndex(table.getPath(), name + "-file-fulltext-content");
		}
		if (indexFileVersions) {
			versionIndex = new ShortIndex(name + "-file-version", table, columnType);
			versionDataIndex = new FileVersionDataIndex(name, table);
		}
		fullTextIndex = new CollectionTextSearchIndex(getPath(), name);
		filePath = getFQN().replace('.', '/');

		localFileStorePath = new File(table.getPath(), name + "-file-store"); //todo separate base path?

		remoteFileStore = null; //todo
	}

	@Override
	public IndexType getType() {
		return IndexType.FILE_NG;
	}

	@Override
	public FileValue getGenericValue(int id) {
		return getValue(id);
	}

	@Override
	public void setGenericValue(int id, FileValue value) {
		setValue(id, value);
	}

	@Override
	public void removeValue(int id) {
		setValue(id, null);
	}

	public void removeAllVersions(int id) {
		if (indexFileVersions) {
			//todo
		} else {
			removeValue(id);
		}
	}

	public FileValue getValue(int id) {
		String hash = hashIndex.getValue(id);
		if (hash == null) {
			return null;
		} else {
			String filePath = getFilePath(hash);
			File localFile = getLocalFile(filePath);
			long size = getSize(id);
			if (localFile.exists() && localFile.length() == size) {
				return new LocalStoreFileValue(id, this, localFile);
			} else if (remoteFileStore != null){
				try {
					InputStream inputStream = remoteFileStore.getInputStream(filePath);
					FileUtil.decryptAndDecompress(inputStream, localFile, hash);
					return new LocalStoreFileValue(id, this, localFile);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return null;
			} else {
				return null;
			}
		}
	}



	public void setValue(int id, FileValue value) {
		if (value == null) {
			String hash = hashIndex.getValue(id);
			if (indexFileVersions && hash != null) {
				String fileName = nameIndex.getValue(id);
				long size = sizeIndex.getValue(id);
				short version = versionIndex.getValue(id);
				versionDataIndex.addVersionEntry(id, version, hash, fileName, size);
			}
			hashIndex.setValue(id, null);
			nameIndex.setValue(id, null);
			sizeIndex.setValue(id, 0);
		} else {
			String hash = value.getHash();
			String filePath = getFilePath(hash);
			File localFile = getLocalFile(filePath);
			if (!localFile.exists()) {
				try {
					IOUtils.copy(value.getInputStream(), new BufferedOutputStream(new FileOutputStream(localFile)));
					if(remoteFileStore != null) {
						File tempFile = File.createTempFile("temp", ".bin");
						FileUtil.encrypt(localFile, tempFile, hash);
						remoteFileStore.setFile(filePath, tempFile);
					}
				} catch (Exception e) {
					throw new RuntimeException("Error: could not write file to local store:" + filePath, e);
				}
			}
			nameIndex.setValue(id, value.getFileName());
			sizeIndex.setValue(id, value.getSize());
			if (indexFileVersions) {

			}
			if (indexFileContent) {

			} else {

			}
			hashIndex.setValue(id, hash);
		}
	}

	private String getFilePath(String hash) {
		String pathHash = FileUtil.createHash(hash, secret);
		return filePath + pathHash.substring(0, 2) + "/" + pathHash + ".bin";
	}

	private File getLocalFile(String path) {
		return new File(localFileStorePath, path);
	}

	private void writeLocalFile(String path, FileValue value) throws IOException {
		File localFile = getLocalFile(path);
		if (localFile.exists()) {
			return;
		} else {
			localFile.getParentFile().mkdirs();
			try (BufferedOutputStream input = new BufferedOutputStream(new FileOutputStream(localFile), 4096 * 16);
				 BufferedInputStream output = new BufferedInputStream(value.getInputStream(), 4096 * 16)) {
				IOUtils.copy(output, input, 4096 * 4);
			}
		}
	}


	@Override
	public String getHash(int id) {
		return hashIndex.getValue(id);
	}

	@Override
	public String getFileName(int id) {
		return nameIndex.getValue(id);
	}

	@Override
	public long getSize(int id) {
		return sizeIndex.getValue(id);
	}

	@Override
	public InputStream getInputStream(int id) {
		String hash = hashIndex.getValue(id);
		String filePath = getFilePath(hash);
		File localFile = getLocalFile(filePath);
		if (localFile.exists()) {
			try {
				return new FileInputStream(localFile);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else if (true) {
			//todo S3 store...
		}
		return null;
	}

	@Override
	public File getAsFile(int id) {
		String hash = hashIndex.getValue(id);
		String filePath = getFilePath(hash);
		File localFile = getLocalFile(filePath);
		if (localFile.exists()) {
			return localFile;
		} else if (true) {
			//todo S3 store...
		}
		return null;
	}

	@Override
	public File getFileVersion(int id, int version) { //todo should return FileValue
		if (indexFileVersions) {
			FileVersionEntry versionData = versionDataIndex.getVersionData(id, version);
			if (versionData != null) {
				String hash = versionData.getHash();
				String filePath = getFilePath(hash);
				File localFile = getLocalFile(filePath);
				if (localFile.exists()) {
					return localFile;
				} else if (true) {
					//todo S3 store...
				}
			}
		}
		return null;
	}

	@Override
	public int getVersion(int id) {
		return indexFileVersions ? versionIndex.getValue(id) : 0;
	}

	@Override
	public void writeTransactionValue(FileValue value, DataOutputStream dataOutputStream) throws IOException {

	}

	@Override
	public FileValue readTransactionValue(DataInputStream dataInputStream) throws IOException {
		return null;
	}

	@Override
	public List<SortEntry> sortRecords(List<SortEntry> sortEntries, boolean ascending, UserContext userContext) {
		return null;
	}

	@Override
	public void dumpIndex(DataOutputStream dataOutputStream, BitSet records) throws IOException {
		for (int id = records.nextSetBit(0); id >= 0; id = records.nextSetBit(id + 1)) {
			String hash = hashIndex.getValue(id);
			if (hash != null) {
				String name = nameIndex.getValue(id);
				long size = sizeIndex.getValue(id);
				if (indexFileVersions) {
					short value = versionIndex.getValue(id);
					Map<Integer, FileVersionEntry> versionData = versionDataIndex.getVersions(id);
				}
				if (indexFileContent) {
					textContentIndex.getEntryIterator();
					//todo..
				}
				dataOutputStream.writeInt(id);
				DataStreamUtil.writeStringWithLengthHeader(dataOutputStream,hash);
				DataStreamUtil.writeStringWithLengthHeader(dataOutputStream,name);
				dataOutputStream.writeLong(size);
				//DataStreamUtil.writeByteArrayWithLengthHeader(dataOutputStream,metaData);
				//todo...
			}
		}
	}

	@Override
	public void restoreIndex(DataInputStream dataInputStream) throws IOException {
		try {
			int id = dataInputStream.readInt();
			String hash = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
			String name = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
			long size = dataInputStream.readLong();
			//todo...
		} catch (EOFException ignore) {}
	}

	@Override
	public BitSet filter(BitSet records, FileFilter fileFilter) {
		return null;
	}

	@Override
	public void close() {
		fullTextIndex.commit(true);
		hashIndex.close();
		nameIndex.close();
		sizeIndex.close();
		if (textContentIndex != null) {
			textContentIndex.close();
		}
		if (versionIndex != null) {
			versionIndex.close();
			versionDataIndex.close();
		}
	}

	@Override
	public void drop() {
		fullTextIndex.drop();
		hashIndex.drop();
		nameIndex.drop();
		sizeIndex.drop();
		if (textContentIndex != null) {
			textContentIndex.drop();
		}
		if (versionIndex != null) {
			versionIndex.drop();
			versionDataIndex.drop();
		}
	}
}
