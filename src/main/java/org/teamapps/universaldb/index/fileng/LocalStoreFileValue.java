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

import java.io.*;

public class LocalStoreFileValue implements FileValue {

	private final int id;
	private final FileStore fileStore;
	private final File file;

	public LocalStoreFileValue(int id, FileStore fileStore, File file) {
		this.id = id;
		this.fileStore = fileStore;
		this.file = file;
	}

	@Override
	public String getHash() {
		return fileStore.getHash(id);
	}

	@Override
	public String getFileName() {
		return fileStore.getFileName(id);
	}

	@Override
	public long getSize() {
		return fileStore.getSize(id);
	}

	@Override
	public InputStream getInputStream() {
		try {
			return new FileInputStream(file);
		} catch (FileNotFoundException ignored) { /* this is checked before */}
		return null;
	}

	@Override
	public File getAsFile() {
		return file;
	}

	@Override
	public int getVersion() {
		return fileStore.getVersion(id);
	}

	@Override
	public File getFileVersion(int version) {
		return fileStore.getFileVersion(id, version);
	}
}
