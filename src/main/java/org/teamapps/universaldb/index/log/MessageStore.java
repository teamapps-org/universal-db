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
import java.util.ArrayList;
import java.util.List;

public class MessageStore<TYPE extends MessageObject> {

	private final RecordIndex records;
	private final PrimitiveEntryAtomicStore messagePositionStore;
	private final LogIndex logIndex;
	private final LocalFileStore fileStore;
	private PojoObjectDecoder<TYPE> pojoObjectDecoder;


	public MessageStore(File path, String name, PojoObjectDecoder<TYPE> pojoObjectDecoder) {
		File basePath = new File(path, name);
		basePath.mkdir();
		this.records = new RecordIndex(basePath, "records");
		this.messagePositionStore = new PrimitiveEntryAtomicStore(basePath, "positions");
		this.logIndex = new RotatingLogIndex(basePath, "messages");
		this.fileStore = new LocalFileStore(basePath, "file-store");
		this.pojoObjectDecoder = pojoObjectDecoder;
	}

	public int getNextRecordId() {
		return records.createRecord();
	}

	public int addMessage(TYPE message) {
		int id = getNextRecordId();
		addMessage(id, message);
		return id;
	}

	public void addMessage(int id, TYPE message) {
		try {
			byte[] bytes = message.toBytes(fileStore);
			long position = logIndex.writeLog(bytes);
			messagePositionStore.setLong(id, position);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void removeMessage(int id) {
		records.setBoolean(id, false);
		messagePositionStore.setLong(id, (Math.abs(messagePositionStore.getLong(id)) * -1));
	}

	public TYPE readMessage(int id) {
		long position = messagePositionStore.getLong(id);
		if (position >= 0) {
			byte[] bytes = logIndex.readLog(position);
			return pojoObjectDecoder.decode(bytes, fileStore);
		} else {
			return null;
		}
	}

	public List<TYPE> readAllMessages() {
		List<TYPE> messages = new ArrayList<>();
		for (Integer id : records.getRecords()) {
			messages.add(readMessage(id));
		}
		return messages;
	}


}
