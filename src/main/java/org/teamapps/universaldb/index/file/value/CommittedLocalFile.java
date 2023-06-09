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
package org.teamapps.universaldb.index.file.value;

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Supplier;

public class CommittedLocalFile implements FileValue {

	private final File file;
	private final String fileName;
	private final String hash;
	private final long size;
	private final Supplier<FileContentData> contentDataSupplier;
	private FileContentData contentData;

	public CommittedLocalFile(File file, String fileName, String hash, long size, Supplier<FileContentData> contentDataSupplier) {
		this.file = file;
		this.fileName = fileName;
		this.hash = hash;
		this.size = size;
		this.contentDataSupplier = contentDataSupplier;
	}

	@Override
	public FileValueType getType() {
		return FileValueType.COMMITTED_LOCAL_FILE;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return new BufferedInputStream(new FileInputStream(file));
	}

	@Override
	public File getAsFile() {
		try {
			Path path = Files.createTempFile("tmp", "." + getFileExtension());
			Files.copy(file.toPath(), path, StandardCopyOption.REPLACE_EXISTING);
			return path.toFile();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void copyToFile(File file) throws IOException {
		Files.copy(this.file.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public String getFileName() {
		return fileName;
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public String getHash() {
		return hash;
	}

	@Override
	public String getKey() {
		return null;
	}

	@Override
	public FileContentData getFileContentData() {
		if (contentData == null) {
			contentData = contentDataSupplier.get();
		}
		return contentData;
	}

	@Override
	public String getDetectedLanguage() {
		return getFileContentData().getLanguage();
	}
}
