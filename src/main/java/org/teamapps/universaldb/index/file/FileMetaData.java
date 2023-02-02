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

import org.teamapps.universaldb.util.DataStreamUtil;
import org.teamapps.universaldb.index.text.FullTextIndexValue;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileMetaData {

	public final static String FIELD_NAME = "name";
	public final static String FIELD_MIME_TYPE = "mime";
	public final static String FIELD_HASH = "hash";
	public final static String FIELD_CONTENT = "content";
	public final static String FIELD_LANGUAGE = "language";
	public final static String FIELD_META = "language";


	private final String name;
	private final long size;
	private String mimeType;
	private String hash;
	private final List<FileMetaDataEntry> entries = new ArrayList<>();
	private String textContent;
	private String language;

	public FileMetaData(String name, long size) {
		this.name = name;
		this.size = size;
	}

	public FileMetaData(byte[] data) throws IOException {
		this(new DataInputStream(new ByteArrayInputStream(data)));
	}

	public FileMetaData(DataInputStream dataInputStream) throws IOException {
		name = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		size = dataInputStream.readLong();
		mimeType = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		hash = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		int len = dataInputStream.readInt();
		for (int i = 0; i  < len; i++) {
			String property = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
			String value = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
			entries.add(new FileMetaDataEntry(property, value));
		}
		textContent = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		language = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
	}

	public byte[] getMetaDataBytes() {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(bos);
			DataStreamUtil.writeStringWithLengthHeader(dos, name);
			dos.writeLong(size);
			DataStreamUtil.writeStringWithLengthHeader(dos, mimeType);
			DataStreamUtil.writeStringWithLengthHeader(dos, hash);
			dos.writeInt(entries.size());
			for (FileMetaDataEntry entry : entries) {
				DataStreamUtil.writeStringWithLengthHeader(dos, entry.getProperty());
				DataStreamUtil.writeStringWithLengthHeader(dos, entry.getValue());
			}
			DataStreamUtil.writeStringWithLengthHeader(dos, textContent);
			DataStreamUtil.writeStringWithLengthHeader(dos, language);
			return bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public String getMetaDataProperty(String propertyName) {
		return entries.stream()
				.filter(entry -> entry.getProperty().equals(propertyName))
				.map(entry -> entry.getValue())
				.findAny().orElse(null);
	}

	public List<String> getPropertyNames() {
		return entries.stream()
				.map(entry -> entry.getProperty())
				.collect(Collectors.toList());
	}

	public List<FullTextIndexValue> getFullTextIndexData() {
		List<FullTextIndexValue> values = new ArrayList<>();
		values.add(new FullTextIndexValue(FIELD_NAME, name));
		values.add(new FullTextIndexValue(FIELD_MIME_TYPE, mimeType));
		values.add(new FullTextIndexValue(FIELD_HASH, hash));
		values.add(new FullTextIndexValue(FIELD_CONTENT, textContent));
		values.add(new FullTextIndexValue(FIELD_LANGUAGE, language));
		StringBuilder sb = new StringBuilder();
		for (FileMetaDataEntry metaDataEntry : entries) {
			sb.append(metaDataEntry.getValue()).append(" ");
		}
		values.add(new FullTextIndexValue(FIELD_META, sb.toString()));
		return values;
	}

	public String getName() {
		return name;
	}

	public String getFileExtension() {
		if (name == null || !name.contains(".")) {
			return null;
		} else {
			return name.substring(name.lastIndexOf('.'), name.length());
		}
	}

	public String getContentSnipped(int maxLength) {
		if (textContent == null) {
			return null;
		} else {
			int len = Math.min(maxLength, textContent.length());
			return textContent.substring(0, len);
		}
	}

	public long getSize() {
		return size;
	}

	public String getMimeType() {
		return mimeType;
	}

	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	public void addMetaDataEntry(String property, String value) {
		if (property == null || property.isBlank() || value == null || value.isBlank()) {
			return;
		}
		entries.add(new FileMetaDataEntry(property, value));
	}

	public List<FileMetaDataEntry> getEntries() {
		return entries;
	}

	public String getTextContent() {
		return textContent;
	}

	public void setTextContent(String textContent) {
		this.textContent = textContent;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("file name:").append(name).append("\n");
		sb.append("size:").append(size).append("\n");
		sb.append("type:").append(mimeType).append("\n");
		sb.append("hash:").append(hash).append("\n");
		sb.append("language:").append(language).append("\n");
		if (textContent != null) {
			String value = textContent.length() > 1000 ? textContent.substring(0, 1000) : textContent;
			value = value.trim().replace('\n', ' ').replace('\r', ' ');
			sb.append(value).append("\n");
		}
		for (FileMetaDataEntry entry : entries) {
			sb.append("\t").append(entry.getProperty()).append(":").append(entry.getValue()).append("\n");
		}
		return sb.toString();
	}
}
