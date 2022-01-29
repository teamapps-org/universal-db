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

public interface RemoteFileStore {

	InputStream getInputStream(String path) throws Exception;

	File getFile(String path) throws Exception;

	void setInputStream(String path, InputStream inputStream, long length) throws Exception;

	void setFile(String path, File file) throws Exception;

	boolean fileExists(String path);

	void removeFile(String path) throws Exception;

}
