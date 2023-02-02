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
package org.teamapps.universaldb.index.fileng;

import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileFullTextContentEntry {

	private final int recordId;
	private final int version;
	private final int length;
	private final String contentText;

	public FileFullTextContentEntry(int recordId, int version, int length, String contentText) {
		this.recordId = recordId;
		this.version = version;
		this.length = length;
		this.contentText = contentText;
	}

	public FileFullTextContentEntry(DataInputStream dataInputStream) throws IOException {
		recordId = dataInputStream.readInt();
		version = dataInputStream.readInt();
		length = dataInputStream.readInt();
		contentText = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
	}

	public byte[] getIndexValue() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeInt(recordId);
		dos.writeInt(version);
		dos.writeInt(length);
		DataStreamUtil.writeStringWithLengthHeader(dos, contentText);
		return bos.toByteArray();
	}

	public int getRecordId() {
		return recordId;
	}

	public int getVersion() {
		return version;
	}

	public int getLength() {
		return length;
	}

	public String getContentText() {
		return contentText;
	}
}
