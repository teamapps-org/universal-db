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


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

public class RandomAccessStore {

	private final File storeFile;

	public RandomAccessStore(File basePath, String name) {
		storeFile = new File(basePath, name);
	}

	public long getSize() {
		return storeFile.length();
	}

	private RandomAccessFile getRandomAccessFile(boolean write, long pos) throws IOException {
		RandomAccessFile ras = new RandomAccessFile(storeFile, write ? "rw" : "r");
		ras.seek(pos);
		return ras;
	}

	public synchronized void write(long pos, byte[] bytes) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(true, pos);
		ras.write(bytes);
		ras.close();
	}

	public synchronized byte[] read(long pos, int length) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(false, pos);
		byte[] bytes = new byte[length];
		int read = 0;
		while (read < bytes.length) {
			read += ras.read(bytes, read, length - read);
		}
		ras.close();
		return bytes;
	}

	public synchronized void writeString(long pos, String value) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(true, pos);
		if (value == null || value.isEmpty()) {
			ras.writeInt(0);
		} else {
			byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
			ras.writeInt(bytes.length);
			ras.write(bytes);
		}
		ras.close();
	}

	public synchronized String readString(long pos) throws IOException {
		int length = readInt(pos);
		if (length == 0) {
			return null;
		} else {
			byte[] bytes = read(pos + 4, length);
			return new String(bytes, StandardCharsets.UTF_8);
		}
	}


	public synchronized void writeInt(long pos, int value) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(true, pos);
		ras.writeInt(value);
		ras.close();
	}

	public synchronized int readInt(long pos) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(false, pos);
		int value = ras.readInt();
		ras.close();
		return value;
	}

	public synchronized void writeLong(long pos, long value) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(true, pos);
		ras.writeLong(value);
		ras.close();
	}

	public synchronized long readLong(long pos) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(false, pos);
		long value = ras.readLong();
		ras.close();
		return value;
	}

	public synchronized void writeBoolean(long pos, boolean value) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(true, pos);
		ras.writeBoolean(value);
		ras.close();
	}

	public synchronized boolean readBoolean(long pos) throws IOException {
		RandomAccessFile ras = getRandomAccessFile(false, pos);
		boolean value = ras.readBoolean();
		ras.close();
		return value;
	}

	public void drop() {
		try {
			storeFile.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
