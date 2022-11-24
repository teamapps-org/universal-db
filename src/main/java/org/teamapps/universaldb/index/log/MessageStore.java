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
import org.teamapps.universaldb.index.buffer.PrimitiveEntryAtomicStore;
import org.teamapps.universaldb.index.buffer.RecordIndex;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MessageStore<TYPE extends MessageObject> {

	private final RecordIndex records;
	private final PrimitiveEntryAtomicStore messagePositionStore;
	private final LogIndex logIndex;
	private final LocalFileStore fileStore;
	private final PojoObjectDecoder<TYPE> pojoObjectDecoder;
	private final BiConsumer<TYPE, Integer> messageIdHandler;
	private final Function<TYPE, Integer> messageToIdFunction;

	public MessageStore(File path, String name, boolean withFileStore, PojoObjectDecoder<TYPE> pojoObjectDecoder, BiConsumer<TYPE, Integer> messageIdHandler, Function<TYPE, Integer> messageToIdFunction) {
		File basePath = new File(path, name);
		basePath.mkdir();
		this.records = new RecordIndex(basePath, "records");
		this.messagePositionStore = new PrimitiveEntryAtomicStore(basePath, "positions");
		this.logIndex = new RotatingLogIndex(basePath, "messages");
		this.fileStore = withFileStore ? new LocalFileStore(basePath, "file-store") : null;
		this.pojoObjectDecoder = pojoObjectDecoder;
		this.messageIdHandler = messageIdHandler;
		this.messageToIdFunction = messageToIdFunction;
		Runtime.getRuntime().addShutdownHook(new Thread(this::close));
	}

	public int getMessageCount() {
		return records.getCount();
	}

	private int getNextRecordId() {
		return records.createRecord();
	}

	public long getStoreSize() {
		return logIndex.getStoreSize();
	}

	public void saveMessage(TYPE message) {
		boolean create = false;
		Integer id = messageToIdFunction.apply(message);
		if (id == null || id == 0) {
			id = getNextRecordId();
			create = true;
		}
		addMessage(id, message, create);
	}

	public int addMessage(TYPE message) {
		int id = getNextRecordId();
		addMessage(id, message, true);
		return id;
	}

	private synchronized void addMessage(int id, TYPE message, boolean create) {
		try {
			if (create) {
				messageIdHandler.accept(message, id);
			}
			byte[] bytes = message.toBytes(fileStore);
			long position = logIndex.writeLog(bytes);
			messagePositionStore.setLong(id, position);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void updateMessage(TYPE message) {
		Integer id = messageToIdFunction.apply(message);
		addMessage(id, message, false);
	}

	public void deleteMessage(TYPE message) {
		Integer id = messageToIdFunction.apply(message);
		deleteMessage(id);
	}

	public void deleteMessage(int id) {
		records.setBoolean(id, false);
		messagePositionStore.setLong(id, (Math.abs(getMessagePositionStoreLong(id)) * -1));
	}

	public void undeleteMessage(TYPE message) {
		Integer id = messageToIdFunction.apply(message);
		undeleteMessage(id);
	}

	public void undeleteMessage(int id) {
		records.setBoolean(id, true);
		messagePositionStore.setLong(id, (Math.abs(getMessagePositionStoreLong(id))));
	}

	public TYPE readMessage(int id) {
		long position = getMessagePositionStoreLong(id);
		if (position >= 0) {
			byte[] bytes = logIndex.readLog(position);
			return pojoObjectDecoder.decode(bytes, fileStore);
		} else {
			return null;
		}
	}

	private long getMessagePositionStoreLong(int id) {
		return messagePositionStore.getLong(id);
	}

	public List<TYPE> readLastMessages(int messageCount) {
		List<Integer> allRecords = records.getRecords();
		List<Integer> lastMessageIds = allRecords.subList(Math.max(allRecords.size() - messageCount, 0), allRecords.size());
		return readMessagesByMessageIds(lastMessageIds);
	}

	public List<TYPE> readAfterMessageId(int messageId) {
		return readAfterMessageId(messageId, Integer.MAX_VALUE);
	}

	public List<TYPE> readAfterMessageId(int messageId, int maxMessages) {
		List<Integer> messageIds = records.getRecords().stream().filter(id -> id > messageId)
				.limit(maxMessages)
				.collect(Collectors.toList());
		return readMessagesByMessageIds(messageIds);
	}

	public List<TYPE> readBeforeMessageId(int messageId, int messageCount) {
		List<Integer> messageIds = records.getRecords()
				.stream()
				.filter(id -> id < messageId)
				.limit(messageCount)
				.collect(Collectors.toList());
		return readMessagesByMessageIds(messageIds);
	}

	private List<TYPE> readMessagesByMessageIds(List<Integer> messageIds) {
		if (messageIds.isEmpty()) {
			return Collections.emptyList();
		} else {
			List<IndexMessage> indexMessages = messageIds.stream()
					.map(id -> new IndexMessage(id, getMessagePositionStoreLong(id)))
					.collect(Collectors.toList());
			return readIndexMessages(indexMessages);
		}
	}

	private List<TYPE> readIndexMessages(List<IndexMessage> messages) {
		logIndex.readLogs(messages);
		return messages.stream()
				.map(msg -> pojoObjectDecoder.decode(msg.getMessage(), fileStore))
				.collect(Collectors.toList());
	}

	public List<TYPE> readAllMessages() {
		List<IndexMessage> indexMessages = records.getRecords().stream()
				.map(id -> new IndexMessage(id, getMessagePositionStoreLong(id)))
				.collect(Collectors.toList());
		return readIndexMessages(indexMessages);
	}

	public void close() {
		try {
			records.close();
			messagePositionStore.close();
			logIndex.close();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}

	public void drop() {
		try {
			records.drop();
			messagePositionStore.drop();
			logIndex.drop();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
