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
package org.teamapps.universaldb.message;



import org.teamapps.message.protocol.message.Message;
import org.teamapps.message.protocol.message.MessageRecord;
import org.teamapps.message.protocol.model.ModelRegistry;
import org.teamapps.message.protocol.model.PojoObjectDecoder;

import java.io.File;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public interface BaseMessageStore {

	static BaseMessageStore create(File path, String name) {
		return new BaseMessageStoreImpl(path, name);
	}

	static BaseMessageStore create(File path, String name, BaseMessageCache messageCache) {
		return new BaseMessageStoreImpl(path, name, messageCache);
	}

	static BaseMessageStore create(File path, String name, ModelRegistry modelRegistry, BaseMessageCache messageCache, BiConsumer<MessageRecord, MessageChangeType> changeHandler) {
		return new BaseMessageStoreImpl(path, name, modelRegistry, messageCache, changeHandler);
	}

	void save(MessageRecord message);

	void delete(int id);

	void undelete(int id);

	MessageRecord getById(int id);

	MessageRecord getByPosition(long position);

	MessageRecord getLast();

	int getMessageCount();

	int getDeletedCount();

	List<MessageRecord> getAllMessages();

	List<MessageRecord> getPreviousMessages(int id, int limit);

	List<MessageRecord> getNextMessages(int id, int limit);

	List<MessageRecord> getMessageVersions(int id);

	CloseableIterator<MessageRecord> iterate();

	CloseableIterator<MessageRecord> iterateDeleted();

	Stream<MessageRecord> getStream();

	Stream<MessageRecord> getStream(int id);

	boolean isEmpty();

	long getStoreSize();

	void flush();

	void close();

	void drop();
}
