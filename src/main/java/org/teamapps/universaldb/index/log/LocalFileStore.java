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
package org.teamapps.universaldb.index.log;

import org.teamapps.message.protocol.file.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalFileStore implements FileDataReader, FileDataWriter {

	private final File basePath;
	private AtomicInteger idGenerator;

	public LocalFileStore(File path, String name) {
		this.basePath = new File(path, name);
		this.basePath.mkdir();
		this.idGenerator = new AtomicInteger(basePath.listFiles().length + 1);
	}

	@Override
	public FileData readFileData(FileDataType type, String fileName, long length, String descriptor, boolean encrypted, String encryptionKey) {
		return new LocalFileData(fileName, length, descriptor);
	}

	@Override
	public FileData writeFileData(FileData fileData) throws IOException {
		if (fileData.getBasePath().equals(basePath.getPath())) {
			return fileData;
		} else {
			String fileId = "F-" + System.currentTimeMillis() + "-" + Integer.toHexString(idGenerator.incrementAndGet()).toUpperCase() + ".bin";
			File destFile = new File(basePath, fileId);
			fileData.copyToFile(destFile);
			return new LocalFileData(fileData.getFileName(), fileData.getLength(), destFile.getPath());
		}
	}

}
