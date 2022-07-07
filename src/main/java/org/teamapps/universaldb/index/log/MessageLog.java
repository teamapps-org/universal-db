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
import org.teamapps.protocol.schema.MessageObject;
import org.teamapps.protocol.schema.ModelRegistry;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class MessageLog {

	private LogIndex logIndex;
	private FileProvider fileProvider;
	private FileSink fileSink;
	private ModelRegistry modelRegistry;

	public MessageLog(File basePath, String name, boolean rotating, FileProvider fileProvider, FileSink fileSink, ModelRegistry modelRegistry) {
		this.fileProvider = fileProvider;
		this.fileSink = fileSink;
		this.modelRegistry = modelRegistry;
		this.logIndex = rotating ? new RotatingLogIndex(basePath, name) : new DefaultLogIndex(basePath, name);

	}

	public long addMessage(MessageObject message) throws IOException {
		byte[] bytes = message.toBytes(fileSink);
		return logIndex.writeLog(bytes);
	}

	public MessageObject readMessage(long position) throws IOException {
		byte[] bytes = logIndex.readLog(position);
		return new MessageObject(bytes, modelRegistry, fileProvider, null);
	}

	public Iterator<MessageObject> getMessages(long startPos) {
		LogIterator logIterator = logIndex.readLogs(startPos);
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return logIterator.hasNext();
			}

			@Override
			public MessageObject next() {
				byte[] bytes = logIterator.next();
				try {
					return new MessageObject(bytes, modelRegistry, fileProvider, null);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}


	public void close() {
		logIndex.close();
	}
}
