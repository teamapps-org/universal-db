package org.teamapps.universaldb.message;



import org.teamapps.message.protocol.message.Message;
import org.teamapps.message.protocol.model.PojoObjectDecoder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public interface MessageStore<MESSAGE extends Message> {

	static <MESSAGE extends Message> MessageStore<MESSAGE> create(File path, String name, PojoObjectDecoder<MESSAGE> messageDecoder) {
		return new MessageStoreImpl<>(path, name, messageDecoder, null);
	}

	static <MESSAGE extends Message> MessageStore<MESSAGE> create(File path, String name, PojoObjectDecoder<MESSAGE> messageDecoder, MessageCache<MESSAGE> messageCache) {
		return new MessageStoreImpl<>(path, name, messageDecoder, messageCache);
	}

	void save(MESSAGE message) throws IOException;

	void saveSecure(MESSAGE message);

	void delete(int id) throws IOException;

	void undelete(int id) throws IOException;

	MESSAGE getById(int id);

	MESSAGE getByPosition(long position);

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
