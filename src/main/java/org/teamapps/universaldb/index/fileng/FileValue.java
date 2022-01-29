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

import java.io.File;
import java.io.InputStream;
import java.util.Map;

public interface FileValue {

	static FileValue create(File file) {
		return new LocalFileValue(file);
	}

	static FileValue create(File file, String fileName) {
		return new LocalFileValue(file, fileName);
	}

	String getHash();

	String getFileName();

	long getSize();

	InputStream getInputStream();

	File getAsFile();

	int getVersion();

	default int getVersionCount() {
		return getVersion();
	}

	default Map<String, String> parseMetaData() {
		return FileUtil.parseFileMetaData(getInputStream());
	}

	File getFileVersion(int version);
}
