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

import org.teamapps.protocol.file.FileDataReader;
import org.teamapps.protocol.file.FileDataWriter;
import org.teamapps.protocol.message.Message;
import org.teamapps.protocol.model.PojoObjectDecoder;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class MessageLog<TYPE extends Message> {

	private final LogIndex logIndex;
	private final FileDataReader fileDataReader;
	private final FileDataWriter fileDataWriter;
	private PojoObjectDecoder<TYPE> pojoObjectDecoder;

	public MessageLog(File basePath, String name, boolean rotating, FileDataReader fileDataReader, FileDataWriter fileDataWriter, PojoObjectDecoder<TYPE> pojoObjectDecoder) {
		this.fileDataReader = fileDataReader;
		this.fileDataWriter = fileDataWriter;
		this.pojoObjectDecoder = pojoObjectDecoder;
		this.logIndex = rotating ? new RotatingLogIndex(basePath, name) : new DefaultLogIndex(basePath, name);

	}

	public long addMessage(Message message) throws IOException {
		byte[] bytes = message.toBytes(fileDataWriter);
		return logIndex.writeLog(bytes);
	}

	public Message readMessage(long position) throws IOException {
		byte[] bytes = logIndex.readLog(position);
		return pojoObjectDecoder.decode(bytes, fileDataReader);
	}

	public Iterator<TYPE> getMessages(long startPos) {
		LogIterator logIterator = logIndex.readLogs(startPos);
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return logIterator.hasNext();
			}

			@Override
			public TYPE next() {
				byte[] bytes = logIterator.next();
				return pojoObjectDecoder.decode(bytes, fileDataReader);
			}
		};
	}


	public void close() {
		logIndex.close();
	}
}
