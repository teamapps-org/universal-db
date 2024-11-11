/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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

import java.io.File;
import java.util.function.Supplier;

public interface FileStore {

	default Supplier<File> getFileSupplier(String path, String uuid, String hash) {
		return () -> getFile(path, uuid, hash);
	}

	File getFile(String path, String uuid, String hash);

	void setFile(String path, String uuid, String hash, File file);

	void removeFile(String path, String uuid);

	boolean fileExists(String path, String uuid);


}
