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
import java.util.ArrayList;
import java.util.List;

public class RotatingLogIndex implements LogIndex {

	private final File basePath;
	private final String name;
	private final int maxLogFileSize;
	private final int scanUpToFileIndex;
	private int currentFileIndex;
	private int currentFilePosition;
	private DataOutputStream dos;

	public RotatingLogIndex(File basePath, String name) {
		this(basePath, name, 1966_080_000, 10);
	}

	public RotatingLogIndex(File basePath, String name, int maxLogFileSize, int scanUpToFileIndex) {
		this.basePath = basePath;
		this.name = name;
		this.maxLogFileSize = maxLogFileSize;
		this.scanUpToFileIndex = Math.min(1, scanUpToFileIndex);
		init();
	}

	private void init() {
		int index = 0;
		int lastAvailableIndex = -1;
		while (index < scanUpToFileIndex || lastAvailableIndex + 1 == index) {
			if (getLogFile(index).exists()) {
				lastAvailableIndex = index;
			}
			index++;
		}
		try {
			if (lastAvailableIndex < 0) {
				currentFileIndex = 0;
				currentFilePosition = 0;
				File logFile = getLogFile(currentFileIndex);
				dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(logFile, true), 32_000));
			} else {
				currentFileIndex = lastAvailableIndex;
				File logFile = getLogFile(currentFileIndex);
				currentFilePosition = (int) logFile.length();
				dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(logFile, true), 32_000));
			}
		} catch (IOException e) {
			throw new RuntimeException("Error opening log index: " + getLogFile(currentFileIndex).getAbsolutePath(), e);
		}
	}

	private File getLogFile(int fileIndex) {
		return new File(basePath, name + "-" + fileIndex + ".lgx");
	}

	private void checkWritePosition(int length) throws IOException {
		if (currentFilePosition + length + 4 >= maxLogFileSize) {
			dos.close();
			currentFileIndex++;
			currentFilePosition = 0;
			File logFile = getLogFile(currentFileIndex);
			dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(logFile, true), 32_000));
		}
	}

	@Override
	public synchronized long writeLog(byte[] bytes, boolean committed) {
		try {
			checkWritePosition(bytes.length);
			dos.writeInt(bytes.length);
			dos.write(bytes);
			long storePos = getPosition();
			currentFilePosition += bytes.length + 4;
			if (committed) {
				dos.flush();
			}
			return storePos;
		} catch (IOException e) {
			throw new RuntimeException("Error writing log to file:" + getLogFile(currentFileIndex).getAbsolutePath(), e);
		}
	}

	@Override
	public synchronized byte[] readLog(long storePosition) {
		File logFile = null;
		try {
			int fileIndex = getFileIndex(storePosition);
			int filePosition = getFilePos(storePosition);
			logFile = getLogFile(fileIndex);
			if (!logFile.exists() || filePosition >= logFile.length()) {
				return null;
			}
			RandomAccessFile ras = new RandomAccessFile(logFile, "r");
			ras.seek(filePosition);
			int size = ras.readInt();
			byte[] bytes = new byte[size];
			int read = 0;
			while (read < bytes.length) {
				read += ras.read(bytes, read, size - read);
			}
			ras.close();
			return bytes;
		} catch (IOException e) {
			throw new RuntimeException("Error reading log file:" + logFile.getAbsolutePath(), e);
		}
	}

	@Override
	public LogIterator readLogs() {
		List<File> storeFiles = getStoreFiles();
		return new LogIterator(storeFiles, 0, true);
	}

	@Override
	public LogIterator readLogs(long pos) {
		List<File> storeFiles = getStoreFiles();
		return new LogIterator(storeFiles, pos, true);
	}

	private List<File> getStoreFiles() {
		List<File> storeFiles = new ArrayList<>();
		for (int i = 0; i <= currentFileIndex; i++) {
			storeFiles.add(getLogFile(i));
		}
		return storeFiles;
	}

	@Override
	public long getPosition() {
		return calculatePosition(currentFileIndex, currentFilePosition);
	}

	@Override
	public boolean isEmpty() {
		return currentFileIndex == 0 && currentFilePosition == 0;
	}

	@Override
	public void flush() {
		try {
			dos.flush();
		} catch (IOException e) {
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
		close();
		for (File storeFile : getStoreFiles()) {
			if (storeFile.exists()) {
				storeFile.delete();
			}
		}
	}

	public static long calculatePosition(int fileIndex, int filePos) {
		return (((long) fileIndex) << 32) | (filePos & 0xffffffffL);
	}

	public static int getFileIndex(long storePosition) {
		return (int) (storePosition >> 32);
	}

	public static int getFilePos(long storePosition) {
		return (int) storePosition;
	}
}
