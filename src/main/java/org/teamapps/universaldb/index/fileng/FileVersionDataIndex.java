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

import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.binary.BinaryIndex;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FileVersionDataIndex {

	private final BinaryIndex versionDataIndex;

	public FileVersionDataIndex(String name, TableIndex table) {
		versionDataIndex = new BinaryIndex(name + "-file-version-data", table, false, ColumnType.BINARY);
	}

	public void addVersionEntry(int id, int version, String hash, String fileName, long size) {
		try {
			addVersionEntry(id, new FileVersionEntry(version, hash, fileName, size));
		} catch (IOException e) {
			throw new RuntimeException("Error: could not add file version entry", e);
		}
	}

	public void addVersionEntry(int id, FileVersionEntry entry) throws IOException {
		if (entry == null) {
			return;
		}
		byte[] bytes = versionDataIndex.getValue(id);
		byte[] entryBytes = entry.getEntryValue();
		if (bytes == null) {
			versionDataIndex.setValue(id, entryBytes);
		} else {
			byte[] newBytes = new byte[bytes.length + entryBytes.length];
			System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
			System.arraycopy(entryBytes, 0, newBytes, bytes.length, entryBytes.length);
			versionDataIndex.setValue(id, newBytes);
		}
	}

	public FileVersionEntry getVersionData(int id, int version) {
		Map<Integer, FileVersionEntry> versions = getVersions(id);
		if (versions == null) {
			return null;
		} else {
			return versions.get(version);
		}
	}

	public Map<Integer, FileVersionEntry> getVersions(int id) {
		byte[] bytes = versionDataIndex.getValue(id);
		if (bytes == null) {
			return null;
		} else {
			DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
			Map<Integer, FileVersionEntry> versions = new HashMap<>();
			while (true) {
				try {
					FileVersionEntry entry = new FileVersionEntry(dataInputStream);
					versions.put(entry.getVersion(), entry);
				} catch (EOFException e) {
					return versions;
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
			}
		}
	}

	public void close() {
		versionDataIndex.close();
	}

	public void drop() {
		versionDataIndex.drop();
	}
}
