/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
import org.teamapps.message.protocol.model.PojoObjectDecoder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public interface MessageStore<MESSAGE extends MessageRecord> {

	static <MESSAGE extends Message> MessageStore<MESSAGE> create(File path, String name, PojoObjectDecoder<MESSAGE> messageDecoder) {
		return new MessageStoreImpl<>(path, name, messageDecoder, null);
	}

	static <MESSAGE extends Message> MessageStore<MESSAGE> create(File path, String name, PojoObjectDecoder<MESSAGE> messageDecoder, MessageCache<MESSAGE> messageCache) {
		return new MessageStoreImpl<>(path, name, messageDecoder, messageCache);
	}

	static <MESSAGE extends Message> MessageStore<MESSAGE> create(File path, String name, PojoObjectDecoder<MESSAGE> messageDecoder, MessageCache<MESSAGE> messageCache, BiConsumer<MESSAGE, MessageChangeType> changeHandler) {
		return new MessageStoreImpl<>(path, name, messageDecoder, messageCache, changeHandler);
	}

	void save(MESSAGE message);

	void delete(int id);

	void undelete(int id);

	MESSAGE getById(int id);

	MESSAGE getByPosition(long position);

	MESSAGE getLast();

	int getMessageCount();

	int getDeletedCount();

	List<MESSAGE> getAllMessages();

	List<MESSAGE> getPreviousMessages(int id, int limit);

	List<MESSAGE> getNextMessages(int id, int limit);

	List<MESSAGE> getMessageVersions(int id);

	CloseableIterator<MESSAGE> iterate();

	CloseableIterator<MESSAGE> iterateDeleted();

	Stream<MESSAGE> getStream();

	Stream<MESSAGE> getStream(int id);

	boolean isEmpty();

	long getStoreSize();

	void flush();

	void close();

	void drop();
}
