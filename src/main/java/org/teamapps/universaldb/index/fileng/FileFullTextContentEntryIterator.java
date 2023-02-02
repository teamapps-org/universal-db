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
package org.teamapps.universaldb.index.fileng;

import java.io.*;
import java.util.Iterator;

public class FileFullTextContentEntryIterator implements Iterator<FileFullTextContentEntry> {
	private final DataInputStream dataInputStream;
	private FileFullTextContentEntry nextEntry;

	public FileFullTextContentEntryIterator(File indexFile) throws IOException {
		DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile), 128_000));
		this.dataInputStream = dataInputStream;
		fetchNext();
	}

	@Override
	public boolean hasNext() {
		return nextEntry != null;
	}

	@Override
	public FileFullTextContentEntry next() {
		FileFullTextContentEntry next = nextEntry;
		fetchNext();
		return next;
	}

	private void fetchNext() {
		try {
			nextEntry = new FileFullTextContentEntry(dataInputStream);
		} catch (EOFException e) {
			nextEntry = null;
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			dataInputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
