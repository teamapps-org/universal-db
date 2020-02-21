/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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

import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.function.Supplier;

public class FileValue {

	private final String uuid;
	private final String hash;
	private final long size;
	private FileMetaData metaData;
	private Supplier<File> fileSupplier;

	public FileValue(File file) {
		this(file, file.getName());
	}

	public FileValue(File file, String fileName) {
		uuid = "ta" + UUID.randomUUID().toString().replace("-", "").toLowerCase();
		hash = FileUtil.createFileHash(file);
		size = file.length();
		metaData = new FileMetaData(fileName, size);
		fileSupplier = () -> file;
	}

	public FileValue(String uuid, String hash, long size) {
		this.uuid = uuid;
		this.hash = hash;
		this.size = size;
	}

	public FileValue(DataInputStream dataInputStream) throws IOException {
		uuid = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		hash = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		size = dataInputStream.readLong();
		byte[] bytes = DataStreamUtil.readByteArrayWithLengthHeader(dataInputStream);
		if (bytes != null && bytes.length > 0) {
			metaData = new FileMetaData(bytes);
		}
	}

	public String getUuid() {
		return uuid;
	}

	public String getHash() {
		return hash;
	}

	public long getSize() {
		return size;
	}

	public String getFileName() {
		return metaData != null ? metaData.getName() : null;
	}

	public String getMimeType() {
		return metaData != null ? metaData.getMimeType() : null;
	}

	public String getFileExtension() {
		return metaData != null ? metaData.getFileExtension() : null;
	}

	public String getContentSnipped(int maxLength) {
		return metaData != null ? metaData.getContentSnipped(maxLength) : null;
	}

	public FileMetaData getMetaData() {
		return metaData;
	}

	public void setMetaData(FileMetaData metaData) {
		this.metaData = metaData;
	}

	public Supplier<File> getFileSupplier() {
		return fileSupplier;
	}

	public void setFileSupplier(Supplier<File> fileSupplier) {
		this.fileSupplier = fileSupplier;
	}

	public File retrieveFile() {
		if (fileSupplier == null) {
			return null;
		} else {
			return fileSupplier.get();
		}
	}

	public void writeValues(DataOutputStream dataOutputStream) throws IOException {
		DataStreamUtil.writeStringWithLengthHeader(dataOutputStream, uuid);
		DataStreamUtil.writeStringWithLengthHeader(dataOutputStream, hash);
		dataOutputStream.writeLong(size);
		if (metaData == null) {
			dataOutputStream.writeInt(0);
		} else {
			byte[] metaDataBytes = metaData.getMetaDataBytes();
			DataStreamUtil.writeByteArrayWithLengthHeader(dataOutputStream, metaDataBytes);
		}
	}

}
