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

import org.teamapps.message.protocol.file.LocalFileStore;
import org.teamapps.message.protocol.message.Message;
import org.teamapps.message.protocol.message.MessageRecord;
import org.teamapps.message.protocol.model.PojoObjectDecoder;

import java.io.*;
import java.util.Set;

public class MessageStoreIterator<MESSAGE extends MessageRecord> implements CloseableIterator<MESSAGE> {

	private final File storeFile;
	private final PojoObjectDecoder<MESSAGE> messageDecoder;
	private final LocalFileStore localFileStore;
	private final DataInputStream dis;
	private final boolean readDeleted;
	private Set<Long> requestedPositions;
	private MESSAGE message;
	private long currentPosition;

	public MessageStoreIterator(final boolean readDeleted, final long startPos, final File storeFile, final PojoObjectDecoder<MESSAGE> messageDecoder, final LocalFileStore localFileStore) throws IOException {
		this(null, readDeleted, startPos, storeFile, messageDecoder, localFileStore);
	}

	public MessageStoreIterator(Set<Long> requestedPositions, final boolean readDeleted, final long startPos, final File storeFile, final PojoObjectDecoder<MESSAGE> messageDecoder, final LocalFileStore localFileStore) throws IOException {
		this.readDeleted = readDeleted;
		this.storeFile = storeFile;
		this.messageDecoder = messageDecoder;
		this.localFileStore = localFileStore;
		this.requestedPositions = requestedPositions;
		dis = new DataInputStream(new BufferedInputStream(new FileInputStream(storeFile), 8192));
		currentPosition = startPos > 4 ? startPos : 4;
		dis.skipNBytes(currentPosition);
	}

	private void readNext() {
		while (!readMessage()) {
		}
	}

	private boolean readMessage() {
		try {
			boolean deleted = dis.readBoolean();
			long previousPos = dis.readLong();
			long nextPos = dis.readLong();
			int length = dis.readInt();
			byte[] bytes = new byte[length];
			dis.readFully(bytes);
			boolean skip = requestedPositions != null && !requestedPositions.contains(currentPosition);
			currentPosition += length + 21;
			if (skip || nextPos > 0 || (readDeleted != deleted)) {
				return false;
			} else {
				message = messageDecoder.decode(bytes, localFileStore);
				return true;
			}
		} catch (EOFException eof) {
			closeSave();
			return true;
		} catch (IOException e) {
			closeSave();
			e.printStackTrace();
			return true;
		}
	}

	@Override
	public void close() throws Exception {
		dis.close();
	}

	@Override
	public boolean hasNext() {
		if (message == null) {
			readNext();
		}
		return message != null;
	}

	@Override
	public MESSAGE next() {
		MESSAGE result = this.message;
		this.message = null;
		return result;
	}
}
