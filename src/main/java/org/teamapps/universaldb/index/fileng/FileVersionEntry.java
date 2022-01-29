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
package org.teamapps.universaldb.index.fileng;

import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileVersionEntry {
	private final int version;
	private final long size;
	private final String hash;
	private final String fileName;

	public FileVersionEntry(int version, String hash, String fileName, long size) {
		this.version = version;
		this.hash = hash;
		this.fileName = fileName;
		this.size = size;
	}

	public FileVersionEntry(DataInputStream dataInputStream) throws IOException {
		version = dataInputStream.readInt();
		size = dataInputStream.readLong();
		hash = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		fileName = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
	}

	public byte[] getEntryValue() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeInt(version);
		dos.writeLong(size);
		DataStreamUtil.writeStringWithLengthHeader(dos, hash);
		DataStreamUtil.writeStringWithLengthHeader(dos, fileName);
		return bos.toByteArray();
	}

	public int getVersion() {
		return version;
	}

	public long getSize() {
		return size;
	}

	public String getHash() {
		return hash;
	}

	public String getFileName() {
		return fileName;
	}
}
