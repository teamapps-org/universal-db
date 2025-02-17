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
package org.teamapps.universaldb.index.log;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DefaultLogIndexTest {

	private static File tempDir;
	private static DefaultLogIndex logIndex;
	private static String TEST_STRING = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet.";
	private static byte[] TEST_DATA = TEST_STRING.getBytes(StandardCharsets.UTF_8);

	@BeforeClass
	public static void setUp() throws Exception {
		tempDir = Files.createTempDirectory("temp").toFile();
		tempDir.deleteOnExit();
		logIndex = new DefaultLogIndex(tempDir, "log-index");
	}

	public static byte[] createTestValue(int length) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		int idx = 0;
		for (int i = 0; i < length / 4; i++) {
			dos.writeInt(idx);
			idx++;
		}
		return Arrays.copyOf(bos.toByteArray(), length);
	}

	@Test
	public void writeLogUncommitted() {
		Map<Integer, Long> positionMap = new HashMap<>();
		for (int i = 0; i < 1_000; i++) {
			long pos = logIndex.writeLog(TEST_DATA, false);
			positionMap.put(i, pos);
		}
		logIndex.flush();
		for (int i = 0; i < 1_000; i++) {
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertArrayEquals(TEST_DATA, bytes);
			assertEquals(TEST_STRING, new String(bytes, StandardCharsets.UTF_8));
		}
	}

	@Test
	public void writeLog() {
		Map<Integer, Long> positionMap = new HashMap<>();
		for (int i = 0; i < 1_000; i++) {
			long pos = logIndex.writeLog(TEST_DATA);
			positionMap.put(i, pos);
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertEquals(TEST_STRING, new String(bytes, StandardCharsets.UTF_8));
		}
		for (int i = 0; i < 1_000; i++) {
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertArrayEquals(TEST_DATA, bytes);
		}

		positionMap = new HashMap<>();
		for (int i = 0; i < 1_000; i++) {
			long pos = logIndex.writeLog(TEST_DATA);
			positionMap.put(i, pos);
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertEquals(TEST_STRING, new String(bytes, StandardCharsets.UTF_8));
		}
		for (int i = 0; i < 1_000; i++) {
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertArrayEquals(TEST_DATA, bytes);
		}
	}


	@Test
	public void readLog() throws IOException {
		Map<Integer, Long> positionMap = new HashMap<>();
		for (int i = 1; i <= 1_000; i++) {
			byte[] testValue = createTestValue(i);
			long pos = logIndex.writeLog(testValue);
			positionMap.put(i, pos);
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertArrayEquals(testValue, bytes);
		}
	}

	@Test
	public void getPosition() {
		DefaultLogIndex logIndex = new DefaultLogIndex(tempDir, "index-log-pos");
		assertEquals(4, logIndex.getPosition());
		logIndex.writeLog(TEST_DATA);
		assertEquals(4 + TEST_DATA.length + 4, logIndex.getPosition());
	}

	@Test
	public void isEmpty() {
		DefaultLogIndex logIndex = new DefaultLogIndex(tempDir, "index-log-empty");
		assertTrue(logIndex.isEmpty());
		logIndex.writeLog(new byte[1]);
		assertFalse(logIndex.isEmpty());
		logIndex.flush();
		assertFalse(logIndex.isEmpty());
		logIndex.close();
		logIndex = new DefaultLogIndex(tempDir, "index-log-empty");
		assertFalse(logIndex.isEmpty());
		logIndex = new DefaultLogIndex(tempDir, "index-log-empty2");
		logIndex.close();
		logIndex = new DefaultLogIndex(tempDir, "index-log-empty2");
		assertTrue(logIndex.isEmpty());
		logIndex.writeLog(new byte[1]);
		assertFalse(logIndex.isEmpty());
	}

	@Test
	public void readLogs() throws Exception {
		DefaultLogIndex logIndex = new DefaultLogIndex(tempDir, "index-rl");
		Map<Integer, Long> positionMap = new HashMap<>();
		for (int i = 0; i < 1_000; i++) {
			long pos = logIndex.writeLog(TEST_DATA);
			positionMap.put(i, pos);
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertEquals(TEST_STRING, new String(bytes, StandardCharsets.UTF_8));
		}
		LogIterator logIterator = logIndex.readLogs();
		for (int i = 0; i < 1_000; i++) {
			assertTrue(logIterator.hasNext());
			byte[] bytes = logIterator.next();
			assertArrayEquals(TEST_DATA, bytes);
			assertEquals(TEST_STRING, new String(bytes, StandardCharsets.UTF_8));
		}
		assertFalse(logIterator.hasNext());
		logIterator.close();
		for (int n = 1; n < 1_000; n++) {
			Long position = positionMap.get(n);
			logIterator = logIndex.readLogs(position);
			for (int i = n; i < 1_000; i++) {
				assertTrue(logIterator.hasNext());
				byte[] bytes = logIterator.next();
				assertArrayEquals(TEST_DATA, bytes);
			}
		}
		logIndex.close();
		logIndex.drop();
	}

	@Test
	public void readLogs2() throws Exception {
		DefaultLogIndex logIndex = new DefaultLogIndex(tempDir, "index-rl2");
		Map<Integer, Long> positionMap = new HashMap<>();
		for (int i = 0; i < 1_000; i++) {
			byte[] testValue = createTestValue(i);
			long pos = logIndex.writeLog(testValue);
			positionMap.put(i, pos);
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertArrayEquals(testValue, bytes);
		}
		LogIterator logIterator = logIndex.readLogs();
		for (int i = 0; i < 1_000; i++) {
			assertTrue(logIterator.hasNext());
			byte[] bytes = logIterator.next();
			byte[] testValue = createTestValue(i);
			assertArrayEquals(testValue, bytes);
		}
		logIterator.close();
		for (int n = 1; n < 1_000; n+= 100) {
			Long position = positionMap.get(n);
			logIterator = logIndex.readLogs(position);
			for (int i = n; i < 1_000; i++) {
				assertTrue(logIterator.hasNext());
				byte[] bytes = logIterator.next();
				byte[] testValue = createTestValue(i);
				assertArrayEquals(testValue, bytes);
			}
		}
		logIndex.close();
		logIndex.drop();
	}

}
