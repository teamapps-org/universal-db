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
package org.teamapps.universaldb.index.file.store;

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.file.value.CommittedLocalFile;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

public class LocalFileStore implements FileStore{

	private final File basePath;
//	private final Function<Integer, FileContentData> contentDataProvider;

	public LocalFileStore(File path, String name) {
		basePath = new File(path, name);
		basePath.mkdir();
	}

	@Override
	public boolean isEncrypted() {
		return false;
	}

	@Override
	public FileValue getFile(String name, String hash, long length, String key, Supplier<FileContentData> contentDataSupplier) {
		File storeFile = getStoreFile(hash, length);
		return new CommittedLocalFile(storeFile, name, hash, length, contentDataSupplier);
	}

	@Override
	public FileValue storeFile(FileValue fileValue) throws IOException {
		if (fileValue == null || fileValue.getSize() == 0) {
			return null;
		}
		File filePath = createPath(fileValue.getHash(), fileValue.getSize());
		if (!filePath.exists() || filePath.length() != fileValue.getSize()) {
			fileValue.copyToFile(filePath);
		}
		return new CommittedLocalFile(filePath, fileValue.getFileName(), fileValue.getHash(), fileValue.getSize(), fileValue::getFileContentData);
	}

	private File getStoreFile(String hash, long length) {
		return FileStoreUtil.getPath(basePath, hash, length);
	}

	private File createPath(String hash, long length) {
		File filePath = getStoreFile(hash, length);
		if (!filePath.getParentFile().exists()) {
			filePath.getParentFile().mkdirs();
		}
		return filePath;
	}


}
