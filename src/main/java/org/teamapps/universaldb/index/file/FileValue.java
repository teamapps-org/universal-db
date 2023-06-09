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
package org.teamapps.universaldb.index.file;

import org.apache.commons.io.IOUtils;
import org.teamapps.message.protocol.utils.MessageUtils;
import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.value.FileValueType;
import org.teamapps.universaldb.index.file.value.MimeType;
import org.teamapps.universaldb.index.file.value.UncommittedFile;
import org.teamapps.universaldb.index.text.FullTextIndexValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.*;

public interface FileValue {



	static FileValue create(File file) {
		return new UncommittedFile(file);
	}

	static FileValue create(File file, String fileName) {
		return new UncommittedFile(file, fileName);
	}

	default void writeValues(DataOutputStream dos) throws IOException {
		if (getType() == FileValueType.UNCOMMITTED_FILE) {
			throw new RuntimeException("Error: cannot serialize uncommitted file");
		}
		MessageUtils.writeString(dos, getFileName());
		MessageUtils.writeString(dos, getHash());
		dos.writeLong(getSize());
		MessageUtils.writeString(dos, getKey());
		FileContentData contentData = getFileContentData();
		dos.writeBoolean(contentData != null);
		if (contentData != null) {
			contentData.write(dos, null);
		}
	}

	FileValueType getType();

	InputStream getInputStream() throws IOException;

	default File getAsFile() {
		try {
			Path path = Files.createTempFile("tmp", "." + getFileExtension());
			Files.copy(getInputStream(), path, StandardCopyOption.REPLACE_EXISTING);
			return path.toFile();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	default String getFileExtension() {
		String name = getFileName();
		int pos = name.lastIndexOf('.');
		return pos > 0 ? name.substring(pos).toLowerCase() : "tmp";
	}

	default void copyToFile(File file) throws IOException {
		Files.copy(getInputStream(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	default byte[] toBytes() throws IOException {
		if (getSize() > 100_000_000) {
			throw new RuntimeException("File too large for byte array:" + getSize());
		}
		BufferedInputStream bis = new BufferedInputStream(getInputStream());
		return IOUtils.readFully(bis, (int) getSize());
	}

	String getFileName();

	long getSize();

	String getHash();

	String getKey();

	default byte[] getHashBytes() {
		return HexFormat.of().parseHex(getHash());
	}

	default byte[] getKeyBytes() {
		return getKey() != null ? HexFormat.of().parseHex(getKey()) : null;
	}

	FileContentData getFileContentData();

	default FileContentData getFileContentData(int maxContentLength) {
		return getFileContentData();
	}

	String getDetectedLanguage();

	default MimeType getMimeTypeData() {
		MimeType mimeType = MimeType.getMimeType(getFileName());
		if (mimeType == null && getFileContentData() != null) {
			mimeType = MimeType.getMimeTypeByMime(getFileContentData().getMimeType());
		}
		return mimeType;
	}

	default List<FullTextIndexValue> getFullTextIndexData() {
		List<FullTextIndexValue> fullTextIndexValues = new ArrayList<>();
		fullTextIndexValues.add(new FullTextIndexValue(FileDataField.NAME.name(), getFileName()));
		String fileExtension = getFileExtension();
		if (fileExtension != null && !fileExtension.isBlank()) {
			fullTextIndexValues.add(new FullTextIndexValue(FileDataField.EXTENSION.name(), fileExtension));
		}
		FileContentData contentData = getFileContentData();
		if (contentData.getContent() != null) {
			fullTextIndexValues.add(new FullTextIndexValue(FileDataField.CONTENT.name(), contentData.getContent()));
		}
		if (contentData.getMetaValues() != null) {
			String metaData = String.join(", ", contentData.getMetaValues());
			fullTextIndexValues.add(new FullTextIndexValue(FileDataField.META_DATA.name(), metaData));
		}
		return fullTextIndexValues;
	}


	default String getMimeType() {
		return getFileContentData() == null ? null : getFileContentData().getMimeType();
	}

	default String getTextContent() {
		return getFileContentData() == null ? null : getFileContentData().getContent();
	}

	default String getLatitude() {
		return getFileContentData() == null ? null : getFileContentData().getLatitude();
	}

	default String getLongitude() {
		return getFileContentData() == null ? null : getFileContentData().getLongitude();
	}

	default int getPageCount() {
		return getFileContentData() == null ? 0 : getFileContentData().getPages();
	}

	default int getImageWidth() {
		return getFileContentData() == null ? 0 : getFileContentData().getImageWidth();
	}

	default int getImageHeight() {
		return getFileContentData() == null ? 0 : getFileContentData().getImageHeight();
	}

	default String getContentCreatedBy() {
		return getFileContentData() == null ? null : getFileContentData().getCreatedBy();
	}

	default Instant getContentCreationDate() {
		return getFileContentData() == null ? null : getFileContentData().getDateCreated();
	}

	default String getContentModifiedBy() {
		return getFileContentData() == null ? null : getFileContentData().getModifiedBy();
	}

	default Instant getContentModificationDate() {
		return getFileContentData() == null ? null : getFileContentData().getDateModified();
	}

	default String getDevice() {
		return getFileContentData() == null ? null : getFileContentData().getDevice();
	}

	default String getSoftware() {
		return getFileContentData() == null ? null : getFileContentData().getSoftware();
	}
}
