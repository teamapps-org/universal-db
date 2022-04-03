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
package org.teamapps.universaldb.index.buffer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class BlockEntryAtomicStoreTest {

	private static BlockEntryAtomicStore store;

	@BeforeClass
	public static void setup() throws IOException {
		File tempDir = Files.createTempDirectory("temp").toFile();
		tempDir.deleteOnExit();
		store = new BlockEntryAtomicStore(tempDir, "blockEntryAtomicStoreTest");
	}

	@AfterClass
	public static void tearDown() {
		store.drop();
	}

	@Test
	public void setBytes() {
		byte[] bytes = new byte[50];
		store.setBytes(1, bytes);
		assertEquals(50, store.getBytes(1).length);
	}

	@Test
	public void getBytes() {
		byte[] bytes = new byte[3];
		store.setBytes(1, bytes);
		assertEquals(3, store.getBytes(1).length);
	}

	@Test
	public void removeBytes() {
		byte[] bytes = new byte[50];
		store.setBytes(1, bytes);
		assertEquals(50, store.getBytes(1).length);
		store.removeBytes(1);
		assertEquals(null, store.getBytes(1));
	}

	@Test
	public void setText() {
		for (int id = 1; id < 10_000; id++) {
			store.setText(id, "value" + id);
		}
		for (int id = 1; id < 10_000; id++) {
			assertEquals("value" + id, store.getText(id));
		}
	}

	@Test
	public void getText() {
		StringBuilder sb = new StringBuilder();
		for (int id = 1; id < 10_000; id++) {
			sb.append(id).append("-");
		}
		String value = sb.toString();
		store.setText(1, value);
		assertEquals(value, store.getText(1));
	}

	@Test
	public void removeText() {
		store.setText(1, "test");
		assertEquals("test", store.getText(1));
		store.removeText(1);
		assertEquals(null, store.getText(1));
	}
}
