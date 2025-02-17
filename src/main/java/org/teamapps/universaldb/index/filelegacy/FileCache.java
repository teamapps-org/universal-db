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
package org.teamapps.universaldb.index.filelegacy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.Map;

public class FileCache {


	private static int MAX_FILES = 100_000;
	private static long MAX_CACHE_SIZE = 10 * 1000_000_000L;

	private final Logger log = LoggerFactory.getLogger(FileCache.class);
	private final File tempPath;
	private final Map<String, File> fileMap = new LinkedHashMap<String, File>(1000, 0.75f, true) {
		@Override
		protected boolean removeEldestEntry(Map.Entry<String, File> eldest) {
			if (cacheSize > MAX_CACHE_SIZE || size() > MAX_FILES) {
				cacheSize -= eldest.getValue().length();
				removeFile(eldest.getValue());
				return true;
			}
			return false;
		}
	};
	private long cacheSize;

	public FileCache(File tempPath) {
		this.tempPath = new File(tempPath, "file-cache");
		tempPath.mkdir();
	}

	public File getFile(String filePath) {
		return fileMap.get(filePath);
	}

	public void addFile(File file, String filePath) throws IOException {
		if (file.length() > MAX_CACHE_SIZE) {
			return;
		}
		if (getFile(filePath) != null) {
			return;
		}
		File cacheFilePath = createCacheFilePath(filePath);
		if (cacheFilePath.exists()) {
			fileMap.put(filePath, file);
			return;
		}
		Files.copy(file.toPath(), cacheFilePath.toPath());
		cacheSize += cacheFilePath.length();
		fileMap.put(filePath, cacheFilePath);
	}

	public void putFinishedCacheFile(File file, String filePath) {
		cacheSize += file.length();
		fileMap.put(filePath, file);
	}

	private void removeFile(File file) {
		file.delete();
	}

	public File createCacheFilePath(String filePath) {
		File path = new File(tempPath, filePath);
		File dir = path.getParentFile();
		if (!dir.exists()) {
			dir.mkdirs();
			log.info("Create file cache dir:" + dir.getPath());
		}
		return path;
	}

	private File createPathFromHash(String hash) {
		File dir = new File(tempPath, hash.substring(0, 2));
		if (!dir.exists()) {
			dir.mkdir();
		}
		return new File(dir, hash);
	}
}
