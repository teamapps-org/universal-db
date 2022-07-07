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
package org.teamapps.universaldb.index.log;

import org.teamapps.protocol.file.FileProvider;
import org.teamapps.protocol.file.FileSink;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalFileStore implements FileSink, FileProvider {

	private final File basePath;
	private AtomicInteger idGenerator;

	public LocalFileStore(File path, String name) {
		this.basePath = new File(path, name);
		this.basePath.mkdir();
		this.idGenerator = new AtomicInteger(basePath.listFiles().length + 1);
	}

	public String saveFile(File file) throws IOException {
		if (file == null || file.length() == 0) {
			return null;
		} else {
			String fileId = "F-" + System.currentTimeMillis() + "-" + Integer.toHexString(idGenerator.incrementAndGet()).toUpperCase() + ".bin";
			File destFile = new File(basePath, fileId);
			Files.copy(file.toPath(), destFile.toPath());
			return fileId;
		}
	}

	@Override
	public File getFile(String fileId) {
		return new File(basePath, fileId);
	}

	@Override
	public String handleFile(File file) throws IOException {
		return saveFile(file);
	}
}
