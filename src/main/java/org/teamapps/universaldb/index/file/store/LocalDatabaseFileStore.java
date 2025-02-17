/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
package org.teamapps.universaldb.index.file.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class LocalDatabaseFileStore implements DatabaseFileStore {

	private final File basePath;

	public LocalDatabaseFileStore(File basePath) {
		this.basePath = basePath;
	}

	@Override
	public boolean isEncrypted() {
		return false;
	}

	@Override
	public File getLocalFile(String hash, long length, String key) {
		return FileStoreUtil.getPath(basePath, hash, length);
	}

	@Override
	public File loadRemoteFile(String hash, long length, String key) {
		return null;
	}

	@Override
	public String storeFile(File file, String hash, long length)  {
		try {
			File storeFile = FileStoreUtil.getPath(basePath, hash, length, true);
			if (!storeFile.exists() || storeFile.length() != length) {
				Files.copy(file.toPath(), storeFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
			return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
