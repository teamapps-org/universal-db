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

import org.teamapps.message.protocol.file.LocalFileStore;
import org.teamapps.message.protocol.message.MessageRecord;
import org.teamapps.message.protocol.model.PojoObjectDecoder;
import org.teamapps.universaldb.index.buffer.common.PrimitiveEntryAtomicStore;

import java.io.*;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.teamapps.universaldb.message.MessageChangeType.CREATE;
import static org.teamapps.universaldb.message.MessageChangeType.UPDATE;

public class MessageStoreImpl<MESSAGE extends MessageRecord> implements MessageStore<MESSAGE> {

	private final File storeFile;
	private final DataOutputStream dos;
	private final PrimitiveEntryAtomicStore messagePositions;
	private final PojoObjectDecoder<MESSAGE> messageDecoder;
	private final LocalFileStore localFileStore;
	private final MessageCache<MESSAGE> messageCache;
	private final BiConsumer<MESSAGE, MessageChangeType> changeHandler;
	private int lastId;
	private long position;


	public MessageStoreImpl(File path, String name, PojoObjectDecoder<MESSAGE> messageDecoder) {
		this(path, name, messageDecoder, null, null);
	}

	public MessageStoreImpl(File path, String name, PojoObjectDecoder<MESSAGE> messageDecoder, MessageCache<MESSAGE> messageCache) {
		this(path, name, messageDecoder, messageCache, null);
	}

	public MessageStoreImpl(File path, String name, PojoObjectDecoder<MESSAGE> messageDecoder, MessageCache<MESSAGE> messageCache, BiConsumer<MESSAGE, MessageChangeType> changeHandler) {
		File basePath = new File(path, name);
		basePath.mkdir();
		this.messageDecoder = messageDecoder;
		this.localFileStore = new LocalFileStore(basePath, "file-store");
		this.storeFile = new File(basePath, "messages.msx");
		this.position = storeFile.length();
		this.messagePositions = new PrimitiveEntryAtomicStore(basePath, "pos");
		this.changeHandler = changeHandler;
		this.dos = init();
		if (messageCache != null && messageCache.isFullCache()) {
			getStream().forEach(message -> messageCache.addMessage(message.getRecordId(), false, message));
		}
		this.messageCache = messageCache;
		Runtime.getRuntime().addShutdownHook(new Thread(this::close));
	}

	private DataOutputStream init() {
		try {
			lastId = (int) messagePositions.getLong(0);
			DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(storeFile, true), 16_000));
			if (position == 0) {
				dataOutputStream.writeInt((int) (System.currentTimeMillis() / 1000));
				position = 4;
			} else {
				position = storeFile.length();
			}
			return dataOutputStream;
		} catch (IOException e) {
			throw new RuntimeException("Error creating log index", e);
		}
	}

	@Override
	public synchronized void save(MESSAGE message)  {
		try {
			int recordId = message.getRecordId();
			long previousPos = 0;
			MessageChangeType changeType = UPDATE;
			if (recordId == 0) {
				changeType = CREATE;
				recordId = ++lastId;
				messagePositions.setLong(0, recordId);
				message.setRecordId(recordId);
				message.setRecordModificationDate(Instant.now());
			} else {
				previousPos = messagePositions.getLong(recordId);
				message.setRecordModificationDate(Instant.now());
			}
			if (changeHandler != null) {
				changeHandler.accept(message, changeType);
			}
			byte[] bytes = message.toBytes(localFileStore, true);
			dos.writeBoolean(false);
			dos.writeLong(previousPos);
			dos.writeLong(0);
			dos.writeInt(bytes.length);
			dos.write(bytes);
			long storePos = position;
			position += bytes.length + 21;
			dos.flush();
			messagePositions.setLong(recordId, storePos);

			if (previousPos > 0) {
				RandomAccessFile ras = new RandomAccessFile(storeFile, "rw");
				ras.seek(previousPos + 9);
				ras.writeLong(storePos);
				ras.close();
			}
			if (messageCache != null) {
				messageCache.addMessage(recordId, previousPos > 0, message);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void delete(int id) {
		try {
			long pos = messagePositions.getLong(id);
			if (pos > 0) {
				messagePositions.setLong(id, pos * -1);
				RandomAccessFile ras = new RandomAccessFile(storeFile, "rw");
				ras.seek(pos);
				ras.writeBoolean(true);
				ras.close();

				if (messageCache != null) {
					messageCache.removeMessage(id);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public synchronized void undelete(int id) {
		try {
			long pos = messagePositions.getLong(id);
			if (pos < 0) {
				messagePositions.setLong(id, pos * -1);
				RandomAccessFile ras = new RandomAccessFile(storeFile, "rw");
				ras.seek(pos * -1);
				ras.writeBoolean(false);
				ras.close();

				if (messageCache != null) {
					messageCache.removeMessage(id);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public MESSAGE getById(int id) {
		if (messageCache != null) {
			MESSAGE message = messageCache.getMessage(id);
			if (message == null) {
				long pos = messagePositions.getLong(id);
				message= getByPosition(pos);
				if (message != null) {
					messageCache.addMessage(id, false, message);
				}
				return message;
			} else {
				return message;
			}
		} else {
			long pos = messagePositions.getLong(id);
			return getByPosition(pos);
		}
	}

	@Override
	public MESSAGE getByPosition(long pos) {
		if (pos <= 0) {
			return null;
		}
		try {
			RandomAccessFile ras = new RandomAccessFile(storeFile, "r");
			ras.seek(pos + 17);
			int size = ras.readInt();
			byte[] bytes = new byte[size];
			int read = 0;
			while (read < bytes.length) {
				read += ras.read(bytes, read, size - read);
			}
			ras.close();
			return messageDecoder.decode(bytes, localFileStore);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public MESSAGE getLast() {
		return getById(lastId);
	}

	@Override
	public int getMessageCount() {
		if (messageCache != null && messageCache.isFullCache()) {
			return messageCache.getMessageCount();
		} else {
			return getMessagePositions(false, 0, false, Integer.MAX_VALUE).size();
		}
	}

	@Override
	public int getDeletedCount() {
		return getMessagePositions(false, 0, true, Integer.MAX_VALUE).size();
	}

	private List<MESSAGE> readMessages(boolean backwards, int startId, boolean deleted, int limit) {
		List<MessagePosition<MESSAGE>> positionList = getMessagePositions(backwards, startId, deleted, limit);
		if (positionList.isEmpty()) {
			return Collections.emptyList();
		} else {
			Set<Long> requestedPositions = positionList.stream().map(MessagePosition::getPosition).collect(Collectors.toSet());
			long startPosition = positionList.stream().mapToLong(MessagePosition::getPosition).min().orElse(0);
			CloseableIterator<MESSAGE> iterator = createIterator(deleted, startPosition, requestedPositions);
			List<MESSAGE> messages = new ArrayList<>();
			while (iterator.hasNext() && messages.size() < limit) {
				messages.add(iterator.next());
			}
			return messages.stream().sorted(Comparator.comparingInt(MessageRecord::getRecordId)).collect(Collectors.toList());
		}
	}

	private List<MessagePosition<MESSAGE>> getMessagePositions(boolean backwards, int startId, boolean deleted, int limit) {
		List<MessagePosition<MESSAGE>> positions = new ArrayList<>();
		int length = backwards ? startId : lastId + 1 - startId;
		for (int i = 0; i < length; i++) {
			int id = backwards ? startId - i : startId + i;
			long position = messagePositions.getLong(id);
			if (position > 0 && !deleted) {
				positions.add(new MessagePosition<>(id, position));
			} else if (position < 0 && deleted) {
				positions.add(new MessagePosition<>(id, Math.abs(position)));
			}
			if (positions.size() == limit) {
				return positions;
			}
		}
		return positions;
	}

	@Override
	public List<MESSAGE> getAllMessages() {
		if (messageCache != null && messageCache.isFullCache()) {
			return messageCache.getMessages();
		} else {
			return getStream().collect(Collectors.toList());
		}
	}

	@Override
	public List<MESSAGE> getPreviousMessages(int id, int limit) {
		return readMessages(true, id, false, limit);
	}

	@Override
	public List<MESSAGE> getNextMessages(int id, int limit) {
		return readMessages(false, id, false, limit);
	}

	@Override
	public List<MESSAGE> getMessageVersions(int id) {
		List<MESSAGE> messages = new ArrayList<>();
		long pos = messagePositions.getLong(id);
		try (RandomAccessFile ras = new RandomAccessFile(storeFile, "r")) {
			while (pos > 0) {
				try {
					ras.seek(pos);
					boolean deleted = ras.readBoolean();
					long previousPos = ras.readLong();
					long nextPos = ras.readLong();
					pos = previousPos;
					int size = ras.readInt();
					byte[] bytes = new byte[size];
					int read = 0;
					while (read < bytes.length) {
						read += ras.read(bytes, read, size - read);
					}
					messages.add(messageDecoder.decode(bytes, localFileStore));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return messages;
	}

	@Override
	public CloseableIterator<MESSAGE> iterate() {
		return createIterator(false, 0);
	}

	@Override
	public CloseableIterator<MESSAGE> iterateDeleted() {
		return createIterator(true, 0);
	}

	private CloseableIterator<MESSAGE> createIterator(boolean readDeleted, long startPos) {
		return createIterator(readDeleted, startPos, null);
	}

	private CloseableIterator<MESSAGE> createIterator(boolean readDeleted, long startPos, Set<Long> requestedPositions) {
		try {
			return new MessageStoreIterator<>(requestedPositions, readDeleted, startPos, storeFile, messageDecoder, localFileStore);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Stream<MESSAGE> getStream() {
		return getStream(false, 0);
	}

	@Override
	public Stream<MESSAGE> getStream(int id) {
		return getStream(false, id);
	}

	private Stream<MESSAGE> getStream(boolean readDeleted, long startPos) {
		CloseableIterator<MESSAGE> iterator = createIterator(readDeleted, startPos);
		return StreamSupport
				.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
				.onClose(iterator::closeSave)
				;
	}

	@Override
	public boolean isEmpty() {
		return position <= 4;
	}

	@Override
	public long getStoreSize() {
		return storeFile.length();
	}

	@Override
	public void flush() {
		try {
			messagePositions.flush();
			dos.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
			messagePositions.close();
			dos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void drop() {
		try {
			close();
			storeFile.delete();
			messagePositions.drop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
