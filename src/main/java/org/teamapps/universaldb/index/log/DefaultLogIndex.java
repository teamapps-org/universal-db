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



import java.io.*;
import java.util.Collections;

public class DefaultLogIndex implements LogIndex {
	private final File storeFile;
	private final DataOutputStream dos;
	private long position;

	public DefaultLogIndex(File basePath, String name) {
		storeFile = new File(basePath, name);
		position = storeFile.length();
		dos = createIndexFile();
	}

	private DataOutputStream createIndexFile() {
		try {
			DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(storeFile, true), 16_000));
			if (position == 0) {
				dataOutputStream.writeInt((int) (System.currentTimeMillis() / 1000));
				position = 4;
			}
			return dataOutputStream;
		} catch (IOException e) {
			throw new RuntimeException("Error creating log index", e);
		}
	}

	@Override
	public synchronized long writeLog(byte[] bytes, boolean committed) {
		try {
			dos.writeInt(bytes.length);
			dos.write(bytes);
			long storePos = position;
			position += bytes.length + 4;
			if (committed) {
				dos.flush();
			}
			return storePos;
		} catch (IOException e) {
			throw new RuntimeException("Error writing log to file", e);
		}
	}

	@Override
	public synchronized byte[] readLog(long pos) {
		try {
			RandomAccessFile ras = new RandomAccessFile(storeFile, "r");
			ras.seek(pos);
			int size = ras.readInt();
			byte[] bytes = new byte[size];
			int read = 0;
			while (read < bytes.length) {
				read += ras.read(bytes, read, size - read);
			}
			ras.close();
			return bytes;
		} catch (IOException e) {
			throw new RuntimeException("Error reading log file", e);
		}
	}

	@Override
	public LogIterator readLogs() {
		return new LogIterator(Collections.singletonList(storeFile), 0, false);
	}

	@Override
	public LogIterator readLogs(long pos) {
		return new LogIterator(Collections.singletonList(storeFile), pos, false);
	}

	@Override
	public long getPosition() {
		return position;
	}

	@Override
	public boolean isEmpty() {
		return position <= 4;
	}

	@Override
	public void flush() {
		try {
			dos.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() {
		try {
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
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
