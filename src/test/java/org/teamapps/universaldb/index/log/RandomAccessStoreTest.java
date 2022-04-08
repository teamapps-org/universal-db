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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.*;

public class RandomAccessStoreTest {
	private static File tempDir;
	private static RandomAccessStore randomAccessStore;

	@BeforeClass
	public static void setUp() throws Exception {
		tempDir = Files.createTempDirectory("temp").toFile();
		tempDir.deleteOnExit();
		randomAccessStore = new RandomAccessStore(tempDir, "random-access");
	}

	private byte[] createTestValue(int length) throws IOException {
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
	public void getSize() throws IOException {
		RandomAccessStore store = new RandomAccessStore(tempDir, "random-access-size");
		assertEquals(0, store.getSize());
		store.writeInt(10, 1);
		assertEquals(14, store.getSize());
	}

	@Test
	public void write() throws IOException {
		byte[] testValue = createTestValue(1_000);
		randomAccessStore.write(10_000, testValue);
		byte[] bytes = randomAccessStore.read(10_000, testValue.length);
		assertArrayEquals(testValue, bytes);
	}

	@Test
	public void read() throws IOException {
		byte[] testValue = DefaultLogIndexTest.createTestValue(10_000);
		randomAccessStore.write(1_000, testValue);
		byte[] bytes = randomAccessStore.read(1_000, testValue.length);
		assertArrayEquals(testValue, bytes);
	}

	@Test
	public void writeString() throws IOException {
		randomAccessStore.writeString(123, "abc");
		String value = randomAccessStore.readString(123);
		assertEquals("abc", value);
	}

	@Test
	public void readString() throws IOException {
		String testValue = "this is the test value...";
		randomAccessStore.writeString(123, testValue);
		String value = randomAccessStore.readString(123);
		assertEquals(testValue, value);
		randomAccessStore.writeString(123, null);
		assertEquals(null, randomAccessStore.readString(123));
	}

	@Test
	public void writeInt() throws IOException {
		randomAccessStore.writeInt(123, 123456789);
		assertEquals(123456789, randomAccessStore.readInt(123));
	}

	@Test
	public void readInt() throws IOException {
		for (int i = 0; i < 1_000; i+= 4) {
			randomAccessStore.writeInt(i, i);
		}
		for (int i = 0; i < 1_000; i+= 4) {
			assertEquals(i, randomAccessStore.readInt(i));
		}
	}

	@Test
	public void writeLong() throws IOException {
		randomAccessStore.writeLong(123, 12345678901234L);
		assertEquals(12345678901234L, randomAccessStore.readLong(123));
	}

	@Test
	public void readLong() {
	}

	@Test
	public void writeBoolean() throws IOException {
		randomAccessStore.writeBoolean(123, true);
		assertTrue(randomAccessStore.readBoolean(123));
		randomAccessStore.writeBoolean(123, false);
		assertFalse(randomAccessStore.readBoolean(123));
	}

	@Test
	public void readBoolean() {
	}
}
