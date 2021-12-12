/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
package org.teamapps.universaldb.index.map;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CharIntPersistedMapTest {

	private static CharIntPersistedMap charIntPersistedMap;

	@BeforeClass
	public static void setUp() throws Exception {
		File tempDir = Files.createTempDirectory("temp").toFile();
		tempDir.deleteOnExit();
		charIntPersistedMap = new CharIntPersistedMap(tempDir, "charIntTest");
	}

	@Test
	public void put() {
		for (int i = 0; i < 100_000; i++) {
			charIntPersistedMap.put("" + i, i);
		}
		for (int i = 0; i < 100_000; i++) {
			Integer value = charIntPersistedMap.get("" + i);
			assertNotNull(value);
			long val = value;
			assertEquals(val, i);
		}

	}

	@Test
	public void get() {
	}
}
