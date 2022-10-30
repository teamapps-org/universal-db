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
import java.util.ArrayList;
import java.util.List;

public class ChunkedLogIndex {

	private final LogIndex logIndex;
	private final int entriesPerChunk;

	private long lastEntryPosition = -1;
	private long[] chunks;
	private int currentChunkIndex;
	private int messagesInCurrentChunk;

	public ChunkedLogIndex(File path, String name, int entriesPerChunk, boolean rotatingLogIndex) {
		File basePath = new File(path, name);
		basePath.mkdir();
		this.logIndex = rotatingLogIndex ? new RotatingLogIndex(basePath, name) : new DefaultLogIndex(basePath, name);
		this.entriesPerChunk = entriesPerChunk;
		long[] logPositions = logIndex.readLogPositions();
		int totalEntries = logPositions.length;
		chunks = new long[Math.max(10_000, (totalEntries / entriesPerChunk) * 2)];
		currentChunkIndex = 0;
		if (totalEntries > 0){
			lastEntryPosition = logPositions[logPositions.length - 1];
			currentChunkIndex = -1;
			for (int i = 0; i < totalEntries; i += entriesPerChunk) {
				currentChunkIndex++;
				chunks[currentChunkIndex] = logPositions[i];
			}
		}
		messagesInCurrentChunk = totalEntries - (currentChunkIndex * entriesPerChunk);
	}

	public int getMessagesInCurrentChunk() {
		return messagesInCurrentChunk;
	}

	public synchronized long writeLog(byte[] bytes) {
		long position = logIndex.writeLog(bytes);
		lastEntryPosition = position;
		messagesInCurrentChunk++;
		if (messagesInCurrentChunk > entriesPerChunk) {
			currentChunkIndex++;
			if (currentChunkIndex == chunks.length) {
				long[] newChunks = new long[chunks.length * 2];
				System.arraycopy(chunks, 0, newChunks, 0, chunks.length);
				chunks = newChunks;
			}
			chunks[currentChunkIndex] = position;
			messagesInCurrentChunk = 1;
		}
		return position;
	}

	public byte[] readLog(long pos) {
		return logIndex.readLog(pos);
	}

	public byte[] getLastEntry() {
		if (lastEntryPosition >= 0) {
			return logIndex.readLog(lastEntryPosition);
		} else {
			return null;
		}
	}

	public List<byte[]> readLastLogEntries(int numberOfEntries) {
		numberOfEntries = Math.min(numberOfEntries, getLogEntryCount());
		int chunkCount = (numberOfEntries - 1) / entriesPerChunk;
		if (chunkCount * entriesPerChunk + messagesInCurrentChunk < numberOfEntries) {
			chunkCount++;
		}
		int offset = chunkCount * entriesPerChunk + messagesInCurrentChunk - numberOfEntries;
		long startPos = getPositionByChunkChunkCount(chunkCount);
		LogIterator logIterator = logIndex.readLogs(startPos);
		List<byte[]> logs = new ArrayList<>();
		for (int i = 0; i < offset; i++) {
			logIterator.next();
		}
		for (int i = 0; i < numberOfEntries; i++) {
			logs.add(logIterator.next());
		}
		logIterator.closeSave();
		return logs;
	}

	public int getLogEntryCount() {
		return currentChunkIndex * entriesPerChunk + messagesInCurrentChunk;
	}

	public int getChunkCount() {
		return currentChunkIndex + 1;
	}

	public long getPositionByChunkChunkCount(int chunkCount) {
		return this.chunks[Math.max(0, currentChunkIndex - chunkCount)];
	}

	public void close() {
		logIndex.close();
	}

	public LogIndex getLogIndex() {
		return logIndex;
	}
}
