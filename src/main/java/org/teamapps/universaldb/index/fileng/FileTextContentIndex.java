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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileTextContentIndex {

	private File indexFile;
	private BufferedOutputStream outputStream;

	public FileTextContentIndex(File path, String name) {
		indexFile = new File(path, name + ".idt");
	}

	public void addEntry(int recordId, int version, int length, String contentText) throws IOException {
		addEntry(new FileFullTextContentEntry(recordId, version, length, contentText));
	}

	public void addEntry(FileFullTextContentEntry entry) throws IOException {
		if (outputStream == null) {
			outputStream = new BufferedOutputStream(new FileOutputStream(indexFile), 32_000);
		}
		outputStream.write(entry.getIndexValue());
		outputStream.flush();
	}

	public FileFullTextContentEntryIterator getEntryIterator() throws IOException {
		return new FileFullTextContentEntryIterator(indexFile);
	}

	public void close() {
		if (outputStream != null) {
			try {
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	public void drop() {
		indexFile.delete();
	}
}
