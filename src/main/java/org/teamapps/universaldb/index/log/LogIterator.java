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
import java.util.Iterator;
import java.util.List;

public class LogIterator implements Iterator<byte[]>, AutoCloseable {

	private final List<File> logFiles;
	private int currentFileIndex = -1;
	private DataInputStream dis;
	private byte[] nextLog;
	private long currentReadPos;

	public LogIterator(List<File> logFiles, long startPosition, boolean rotatingLogIndex) {
		this.logFiles = logFiles;
		seekLogPosition(startPosition, rotatingLogIndex);
		readLog();
	}

	private void seekLogPosition(long startPosition, boolean rotatingLogIndex) {
		try {
			long skipBytes = startPosition;
			if (rotatingLogIndex) {
				currentFileIndex = RotatingLogIndex.getFileIndex(startPosition);
				int filePos = RotatingLogIndex.getFilePos(startPosition);
				if (currentFileIndex >= logFiles.size()) {
					return;
				}
				skipBytes = filePos;
			} else {
				currentFileIndex = 0;
			}
			dis = new DataInputStream(new BufferedInputStream(new FileInputStream(logFiles.get(currentFileIndex)), 64_000));
			if (skipBytes > 0) {
				dis.skipNBytes(skipBytes);
				currentReadPos = RotatingLogIndex.calculatePosition(currentFileIndex, (int) skipBytes);
			} else if (!rotatingLogIndex) {
				dis.readInt();
				currentReadPos = 4;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void readLog() {
		try {
			nextLog = null;
			if (dis == null) {
				currentFileIndex++;
				if (currentFileIndex >= logFiles.size()) {
					return;
				}
				dis = new DataInputStream(new BufferedInputStream(new FileInputStream(logFiles.get(currentFileIndex)), 64_000));
				currentReadPos = RotatingLogIndex.calculatePosition(currentFileIndex, 0);
			}
			int size = dis.readInt();
			currentReadPos += size + 4;
			byte[] bytes = new byte[size];
			dis.readFully(bytes);
			nextLog = bytes;
		} catch (EOFException ignore) {
			closeStream();
			dis = null;
			readLog();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public boolean hasNext() {
		if (nextLog != null) {
			return true;
		} else {
			closeStream();
			return false;
		}
	}

	@Override
	public byte[] next() {
		byte[] bytes = nextLog;
		readLog();
		return bytes;
	}

	public long getCurrentReadPosition() {
		return currentReadPos;
	}

	@Override
	public void close() throws Exception {
		if (dis != null) {
			dis.close();
		}
	}

	public void closeSave() {
		try {
			close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void closeStream() {
		try {
			if (dis == null) {
				return;
			}
			dis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
