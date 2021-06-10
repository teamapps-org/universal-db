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
package org.teamapps.universaldb.index.file;

import java.io.File;
import java.nio.file.Files;

public class LocalFileStore extends AbstractFileStore {

	private final File storePath;
	private final boolean encrypt;

	public LocalFileStore(File storePath) {
		this.storePath = storePath;
		this.encrypt = false;
		storePath.mkdir();
	}

	public LocalFileStore(File storePath, boolean encrypt) {
		this.storePath = storePath;
		this.encrypt = encrypt;
	}

	@Override
	public File getFile(String path, String uuid, String hash) {
		try {
			File filePath = createPathFromUuid(path, uuid);
			if (encrypt) {
				File tempFile = File.createTempFile("temp", ".bin");
				FileUtil.decryptAndDecompress(filePath, tempFile, hash);
				return tempFile;
			} else {
				return filePath;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void setFile(String path, String uuid, String hash, File file) {
		try {
			File storeFile = createPathFromUuid(path, uuid);
			if (encrypt) {
				FileUtil.compressAndEncrypt(file, storeFile, hash);
			} else {
				Files.copy(file.toPath(), storeFile.toPath());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void removeFile(String path, String uuid) {
		File storeFile = createPathFromUuid(path, uuid);
		if (storeFile.exists()) {
			storeFile.delete();
		}
	}

	@Override
	public boolean fileExists(String path, String uuid) {
		File storeFile = createPathFromUuid(path, uuid);
		return storeFile.exists();
	}

	private File createPathFromUuid(String path, String uuid) {
		File dir = new File(storePath, path + "/" + uuid.substring(0, 4));
		if (!dir.exists()) {
			dir.mkdirs();
		}
		return new File(dir, uuid);
	}


}
