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

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.*;

public class ChunkedLogIndexTest {

	private static byte[] DATA = "TEST".getBytes(StandardCharsets.UTF_8);
	private static int CHUNK_SIZE = 100;

	private static File createTempDir() throws IOException {
		return Files.createTempDirectory("temp").toFile();
	}

	private static ChunkedLogIndex createIndex() throws IOException {
		File tempDir = createTempDir();
		return createIndex(tempDir);
	}

	private static ChunkedLogIndex createIndex(File path) throws IOException {
		return new ChunkedLogIndex(path, "chunk-index-test", CHUNK_SIZE, true);
	}

	private static boolean checkBytes(byte[] bytes) {
		return bytes != null && new String(bytes, StandardCharsets.UTF_8).equals("TEST");
	}

	@Test
	public void getMessagesInCurrentChunk() throws IOException {
		File path = createTempDir();
		ChunkedLogIndex logIndex = createIndex(path);
		assertEquals(0, logIndex.getLogEntryCount());
		assertEquals(1, logIndex.getChunkCount());
		assertEquals(0, logIndex.getMessagesInCurrentChunk());
		int size = CHUNK_SIZE - 1;
		for (int i = 0; i < size; i++) {
			logIndex.writeLog(DATA);
			assertEquals(1, logIndex.getChunkCount());
			assertEquals(i + 1, logIndex.getLogEntryCount());
			assertEquals(i + 1, logIndex.getMessagesInCurrentChunk());
		}
		logIndex.writeLog(DATA);
		assertEquals(1, logIndex.getChunkCount());
		assertEquals(CHUNK_SIZE, logIndex.getLogEntryCount());
		assertEquals(CHUNK_SIZE, logIndex.getMessagesInCurrentChunk());
		logIndex.close();
		logIndex = createIndex(path);
		assertEquals(1, logIndex.getChunkCount());
		assertEquals(CHUNK_SIZE, logIndex.getLogEntryCount());
		assertEquals(CHUNK_SIZE, logIndex.getMessagesInCurrentChunk());

		logIndex.writeLog(DATA);
		assertEquals(2, logIndex.getChunkCount());
		assertEquals(CHUNK_SIZE + 1, logIndex.getLogEntryCount());
		assertEquals(1, logIndex.getMessagesInCurrentChunk());
		logIndex.close();
		logIndex = createIndex(path);
		assertEquals(2, logIndex.getChunkCount());
		assertEquals(CHUNK_SIZE + 1, logIndex.getLogEntryCount());
		assertEquals(1, logIndex.getMessagesInCurrentChunk());

	}

	@Test
	public void writeLog() throws IOException {
		File path = createTempDir();
		ChunkedLogIndex logIndex = createIndex(path);
		int size = CHUNK_SIZE * 1_000;
		for (int i = 0; i < size; i++) {
			logIndex.writeLog(("" + i).getBytes(StandardCharsets.UTF_8));
		}
		assertEquals(1_000, logIndex.getChunkCount());
		assertEquals(CHUNK_SIZE * 1_000, logIndex.getLogEntryCount());
		assertEquals(CHUNK_SIZE, logIndex.getMessagesInCurrentChunk());

		int len = CHUNK_SIZE * 500 + 50;
		int start = CHUNK_SIZE * 1_000 - len;

		List<byte[]> bytesList = logIndex.readLastLogEntries(len);
		int n = 0;
		assertEquals(len, bytesList.size());
		for (byte[] bytes : bytesList) {
			String val = new String(bytes, StandardCharsets.UTF_8);
			assertEquals("" + (start + n),val);
			n++;
		}
	}

	@Test
	public void readLog() {
	}

	@Test
	public void readLastLogEntries() {
	}

	@Test
	public void getLogEntryCount() {
	}

	@Test
	public void getPositionByChunkChunkCount() {
	}

	@Test
	public void close() {
	}

	@Test
	public void getLogIndex() {
	}
}
