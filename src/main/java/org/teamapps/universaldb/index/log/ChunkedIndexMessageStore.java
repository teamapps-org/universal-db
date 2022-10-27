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

import org.teamapps.protocol.schema.MessageObject;
import org.teamapps.protocol.schema.PojoObjectDecoder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ChunkedIndexMessageStore<TYPE extends MessageObject> {

	private final ChunkedLogIndex messageIndex;
	private final PojoObjectDecoder<TYPE> pojoObjectDecoder;
	private LocalFileStore fileStore;
	private BiConsumer<TYPE, Integer> messageIdHandler;

	public ChunkedIndexMessageStore(File path, String name, int entriesPerChunk, boolean rotatingLogIndex, boolean withFileStore, PojoObjectDecoder<TYPE> pojoObjectDecoder) {
		File basePath = new File(path, name);
		basePath.mkdir();
		this.messageIndex = new ChunkedLogIndex(basePath, "messages", entriesPerChunk, rotatingLogIndex);
		if (withFileStore) {
			this.fileStore = new LocalFileStore(basePath, "file-store");
		}
		this.pojoObjectDecoder = pojoObjectDecoder;
	}

	public void setMessageIdHandler(BiConsumer<TYPE, Integer> messageIdHandler) {
		this.messageIdHandler = messageIdHandler;
	}

	public synchronized long addMessage(TYPE message) {
		try {
			byte[] bytes = message.toBytes(fileStore);
			if (messageIdHandler != null) {
				messageIdHandler.accept(message, messageIndex.getLogEntryCount() + 1);
			}
			return messageIndex.writeLog(bytes);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public TYPE readMessage(long position) {
		if (position >= 0) {
			byte[] bytes = messageIndex.readLog(position);
			return pojoObjectDecoder.decode(bytes, fileStore);
		} else {
			return null;
		}
	}

	public List<TYPE> readLastMessages(int messageCount) {
		List<byte[]> bytes = messageIndex.readLastLogEntries(messageCount);
		return bytes.stream()
				.map(b -> pojoObjectDecoder.decode(b, fileStore))
				.collect(Collectors.toList());
	}

	public TYPE getLastMessage() {
		byte[] bytes = messageIndex.getLastEntry();
		return bytes != null ? pojoObjectDecoder.decode(bytes, fileStore) : null;
	}

	public void close() {
		messageIndex.close();
	}

	public int getMessageCount() {
		return messageIndex.getLogEntryCount();
	}

	public int getChunkCount() {
		return messageIndex.getChunkCount();
	}

	public int getMessagesInCurrentChunk() {
		return messageIndex.getMessagesInCurrentChunk();
	}
}
