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
package org.teamapps.universaldb.index.transaction.resolved.legacy;

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.file.value.StoreDescriptionFile;
import org.teamapps.universaldb.index.reference.value.ResolvedMultiReferenceUpdate;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public class LegacyResolvedTransactionRecordValue {

	public static int fileCounter;
	public static Map<String, Integer> pathCounts = new HashMap<>();
	private static BiFunction<String, Integer, File> fileByUuidAndFieldId;
	private final int columnId;
	private final IndexType indexType;
	private final Object value;

	public LegacyResolvedTransactionRecordValue(int columnId, IndexType indexType, Object value) {
		this.columnId = columnId;
		this.indexType = indexType;
		this.value = value;
	}

	public LegacyResolvedTransactionRecordValue(DataInputStream dis) throws IOException {
		columnId = dis.readInt();
		indexType = IndexType.getIndexTypeById(dis.readByte());
		boolean valueAvailable = dis.readBoolean();
		value = valueAvailable ? readRecordValue(dis) : null;
	}

	public static void setFileByUuidAndFieldId(BiFunction<String, Integer, File> fileByUuidAndFieldId) {
		LegacyResolvedTransactionRecordValue.fileByUuidAndFieldId = fileByUuidAndFieldId;
	}

	public int getColumnId() {
		return columnId;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public Object getValue() {
		return value;
	}

	private Object readRecordValue(DataInputStream dis) throws IOException {
		switch (indexType) {
			case BOOLEAN:
				return dis.readBoolean();
			case SHORT:
				return dis.readShort();
			case INT:
				return dis.readInt();
			case LONG:
				return dis.readLong();
			case FLOAT:
				return dis.readFloat();
			case DOUBLE:
				return dis.readDouble();
			case TEXT:
				return DataStreamUtil.readStringWithLengthHeader(dis);
			case TRANSLATABLE_TEXT:
				return new TranslatableText(dis);
			case REFERENCE:
				return dis.readInt();
			case MULTI_REFERENCE:
				return new ResolvedMultiReferenceUpdate(dis);
			case FILE:
				org.teamapps.universaldb.index.filelegacy.FileValue fileValue = new org.teamapps.universaldb.index.filelegacy.FileValue(dis);
				String fileName = fileValue.getFileName();
				String hash = fileValue.getHash();
				long size = fileValue.getSize();
				String uuid = fileValue.getUuid();
				File file = fileByUuidAndFieldId.apply(uuid, columnId);
				FileContentData contentData = new FileContentData();
				contentData.setFileSize(size);
//				pathCounts.compute(file.getParentFile().getParentFile().getPath(), (k, v) -> v == null ? 1 : v + 1);
//				fileCounter++;
//				if (file.exists()) {
//					fileCounter++;
//					//System.out.println("File: " + file.getPath());
//				} else {
//					System.out.println("ERROR: missing file:"+ file.getPath());
//				}
//				FileMetaData metaData = fileValue.getMetaData();
//				if (metaData == null) {
//					System.out.println("Missing meta data:" + file.getPath());
//				} else {
//					contentData.setMimeType(metaData.getMimeType());
//					contentData.setContent(metaData.getTextContent());
//					contentData.setMetaKeysAsList(metaData.getEntries().stream().map(FileMetaDataEntry::getProperty).collect(Collectors.toList()));
//					contentData.setMetaValuesAsList(metaData.getEntries().stream().map(FileMetaDataEntry::getValue).collect(Collectors.toList()));
//				}
				return new StoreDescriptionFile(file, fileName, size, hash, null, contentData);
			case BINARY:
				return DataStreamUtil.readByteArrayWithLengthHeader(dis);
			case FILE_NG:
				break;
		}
		return null;
	}


	public void write(DataOutputStream dos) throws IOException {
		dos.writeInt(columnId);
		dos.writeByte(indexType.getId());
		if (value == null) {
			dos.writeBoolean(false);
		} else {
			dos.writeBoolean(true);
			switch (indexType) {
				case BOOLEAN:
					dos.writeBoolean((Boolean) value);
					break;
				case SHORT:
					dos.writeShort((Short) value);
					break;
				case INT:
					dos.writeInt((Integer) value);
					break;
				case LONG:
					dos.writeLong((Long) value);
					break;
				case FLOAT:
					dos.writeFloat((Float) value);
					break;
				case DOUBLE:
					dos.writeDouble((Double) value);
					break;
				case TEXT:
					DataStreamUtil.writeStringWithLengthHeader(dos, (String) value);
					break;
				case TRANSLATABLE_TEXT:
					TranslatableText translatableText = (TranslatableText) value;
					translatableText.writeValues(dos);
					break;
				case REFERENCE:
					dos.writeInt((Integer) value);
					break;
				case MULTI_REFERENCE:
					ResolvedMultiReferenceUpdate multiReferenceUpdate = (ResolvedMultiReferenceUpdate) value;
					multiReferenceUpdate.write(dos);
					break;
				case FILE:
					FileValue fileValue = (FileValue) value;
					fileValue.writeValues(dos);
					break;
				case BINARY:
					DataStreamUtil.writeByteArrayWithLengthHeader(dos, (byte[]) value);
					break;
				case FILE_NG:
					break;
			}
		}
	}
}
