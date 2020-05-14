/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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

import org.teamapps.universaldb.index.file.FileUtil;

import java.io.*;

public class LocalFileValue implements FileValue{

	private final File file;
	private final String name;
	private final String hash;

	public LocalFileValue(File file) {
		this(file, file.getName());
	}

	public LocalFileValue(File file, String fileName) {
		this.file = file;
		this.name = fileName;
		this.hash = FileUtil.createFileHash(file);
	}

	@Override
	public String getHash() {
		return hash;
	}

	@Override
	public String getFileName() {
		return name;
	}

	@Override
	public long getSize() {
		return file.length();
	}

	@Override
	public InputStream getInputStream() {
		try {
			return new BufferedInputStream(new FileInputStream(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public File getAsFile() {
		return file;
	}

	/**
	 * The file version. For first version, version is 1.
	 * For uncommitted files an unknown version of 0 might
	 * be returned.
	 *
	 * @return the version or 0, if version is unknown
	 */
	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public File getFileVersion(int version) {
		if (getVersion() == version) {
			return getAsFile();
		} else {
			return null;
		}
	}
}
