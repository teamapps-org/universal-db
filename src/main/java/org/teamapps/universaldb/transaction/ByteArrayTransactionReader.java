/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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
package org.teamapps.universaldb.transaction;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

public class ByteArrayTransactionReader implements Iterator<byte[]> {

	private final File path;
	private int startFileId;
	private int startPosition;
	private int lastFiledId;
	private int lastPosition;
	private int currentFileId;
	private int currentPosition;
	private BufferedInputStream inputStream;

	public ByteArrayTransactionReader(long startTransactionId, long lastTransactionId, File path) throws IOException {
		this.path = path;
		startFileId = TransactionStore.getTransactionFileId(startTransactionId);
		startPosition = TransactionStore.getTransactionFilePosition(startTransactionId);
		lastFiledId = TransactionStore.getTransactionFileId(lastTransactionId);
		lastPosition = TransactionStore.getTransactionFilePosition(lastTransactionId);
		currentFileId = startFileId;
		currentPosition = startPosition;

		nextTransactionFile(currentPosition);
	}

	private void nextTransactionFile(int skipBytes) throws IOException {
		boolean compressed = true;
		File transactionFile = TransactionStore.getTransactionFileByFileId(currentFileId, path, compressed);
		if (!transactionFile.exists()) {
			compressed = false;
			transactionFile = TransactionStore.getTransactionFileByFileId(currentFileId, path, compressed);
		}
		if (compressed) {
			inputStream = new BufferedInputStream(new GZIPInputStream(new FileInputStream(transactionFile)));
			skipBytes(skipBytes);
		} else {
			if (skipBytes == 0) {
				inputStream = new BufferedInputStream(new FileInputStream(transactionFile));
			} else {
				RandomAccessFile raf = new RandomAccessFile(transactionFile, "r");
				raf.seek(skipBytes);
				inputStream = new BufferedInputStream(Channels.newInputStream(raf.getChannel()));
			}
		}
	}

	private void skipBytes(int len) throws IOException {
		if (len == 0) {
			return;
		}
		int read = 0;
		int skipped = 0;
		byte[] buf = new byte[8096];
		while((read = inputStream.read(buf, 0, Math.min(buf.length, len))) >= 0 && len > skipped) {
			skipped += read;
		}
	}

	private byte[] readNextPacket() {
		try {
			byte[] bytes = inputStream.readNBytes(5);
			ByteBuffer buffer = ByteBuffer.wrap(bytes);
			byte type = buffer.get();
			int len = buffer.getInt();
			if (len <= 0 || len > 1000_000_000) {
				return null;
			}
			bytes = inputStream.readNBytes(len);
			if (TransactionStore.newTransactionFileRequired(currentPosition, len + 5)) {
				currentFileId++;
				if (currentFileId > lastFiledId) {
					return bytes;
				}
				currentPosition = 0;
				nextTransactionFile(0);
			} else {
				currentPosition += len + 5;
			}
			return bytes;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean hasNext() {
		return currentFileId < lastFiledId || (currentFileId == lastFiledId && currentPosition <= lastPosition);
	}

	@Override
	public byte[] next() {
		return readNextPacket();
	}
}
