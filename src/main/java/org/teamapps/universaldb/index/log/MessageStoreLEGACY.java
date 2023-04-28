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
package org.teamapps.universaldb.index.log;

import org.teamapps.message.protocol.message.Message;
import org.teamapps.message.protocol.model.PojoObjectDecoder;
import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;
import org.teamapps.universaldb.index.buffer.index.RecordIndex;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MessageStoreLEGACY<TYPE extends Message> {

	private final RecordIndex records;
	private final PrimitiveEntryAtomicStore messagePositionStore;
	private final LogIndex logIndex;
	private final LocalFileStore fileStore;
	private final PojoObjectDecoder<TYPE> pojoObjectDecoder;

	public MessageStoreLEGACY(File path, String name, boolean withFileStore, PojoObjectDecoder<TYPE> pojoObjectDecoder) {
		File basePath = new File(path, name);
		basePath.mkdir();
		this.records = new RecordIndex(basePath, "records");
		this.messagePositionStore = new PrimitiveEntryAtomicStore(basePath, "positions");
		this.logIndex = new RotatingLogIndex(basePath, "messages");
		this.fileStore = withFileStore ? new LocalFileStore(basePath, "file-store") : null;
		this.pojoObjectDecoder = pojoObjectDecoder;
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

	public synchronized void saveMessage(TYPE message) {
		int recordId = message.getRecordId();
		if (recordId <= 0) {
			recordId = getNextRecordId();
			message.setRecordId(recordId);
		}
		addMessage(recordId, message);
	}

	private synchronized void addMessage(int id, TYPE message) {
		try {
			byte[] bytes = message.toBytes(fileStore);
			long position = logIndex.writeLog(bytes);
			messagePositionStore.setLong(id, position);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public synchronized void deleteMessage(TYPE message) {
		deleteMessage(message.getRecordId());
	}

	public synchronized void deleteMessage(int id) {
		records.setBoolean(id, false);
		messagePositionStore.setLong(id, (Math.abs(getMessagePosition(id)) * -1));
	}

	public synchronized void undeleteMessage(TYPE message) {
		undeleteMessage(message.getRecordId());
	}

	public synchronized void undeleteMessage(int id) {
		records.setBoolean(id, true);
		messagePositionStore.setLong(id, (Math.abs(getMessagePosition(id))));
	}

	public TYPE readMessage(int id) {
		long position = getMessagePosition(id);
		if (position >= 0) {
			byte[] bytes = logIndex.readLog(position);
			return pojoObjectDecoder.decode(bytes, fileStore);
		} else {
			return null;
		}
	}

	private long getMessagePosition(int id) {
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
			List<PositionIndexedMessage> positionIndexedMessages = messageIds.stream()
					.map(id -> new PositionIndexedMessage(id, getMessagePosition(id)))
					.collect(Collectors.toList());
			return readIndexMessages(positionIndexedMessages);
		}
	}

	private List<TYPE> readIndexMessages(List<PositionIndexedMessage> messages) {
		logIndex.readLogs(messages);
		return messages.stream()
				.map(msg -> pojoObjectDecoder.decode(msg.getMessage(), fileStore))
				.collect(Collectors.toList());
	}

	public List<TYPE> readAllMessages() {
		List<PositionIndexedMessage> positionIndexedMessages = records.getRecords().stream()
				.map(id -> new PositionIndexedMessage(id, getMessagePosition(id)))
				.collect(Collectors.toList());
		return readIndexMessages(positionIndexedMessages);
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
