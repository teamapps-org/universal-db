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
package org.teamapps.universaldb;

import org.apache.commons.io.IOUtils;
import org.teamapps.datamodel.TestDb1Model;
import org.teamapps.datamodel.TestDb2Model;
import org.teamapps.datamodel.testdb1.FieldTest;
import org.teamapps.universaldb.pojo.Entity;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestBase {

	private static volatile boolean initialized;
	private static volatile boolean initialize2;

	public synchronized static void init() throws Exception {
		if (initialized) {
			return;
		}
		startDb();
		initialized = true;
	}

	public synchronized static void init2() throws Exception {
		if (initialize2) {
			return;
		}
		startDb2();
		initialize2 = true;
	}

	private static void startDb() throws Exception {
		File tempDir = Files.createTempDirectory("temp").toFile();
		tempDir.deleteOnExit();
		UniversalDbBuilder.create().basePath(tempDir).modelProvider(new TestDb1Model()).build();
	}

	private static void startDb2() throws Exception {
		File tempDir1 = Files.createTempDirectory("temp").toFile();
		tempDir1.deleteOnExit();
		File tempDir2 = Files.createTempDirectory("temp").toFile();
		tempDir2.deleteOnExit();
		UniversalDbBuilder.create().basePath(tempDir1).modelProvider(new TestDb1Model()).build();
		UniversalDbBuilder.create().basePath(tempDir2).modelProvider(new TestDb2Model()).build();
	}

	public static File createResourceFile() throws IOException {
		InputStream resourceAsStream = TestBase.class.getResourceAsStream("/org/teamapps/universaldb/test-doc.docx");
		File tempFile = File.createTempFile("temp", ".bin");
		FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
		IOUtils.copyLarge(resourceAsStream, fileOutputStream);
		return tempFile;
	}

	public static boolean check(List<FieldTest> values, Integer ... intValues) {
		if (values.size() != intValues.length) {
			return false;
		}
		List<Integer> integers = Arrays.asList(intValues);
		Set<Integer> set = new HashSet<>(integers);
		for (FieldTest value : values) {
			if (!set.contains(value.getIntField())){
				return false;
			}
		}
		return true;
	}

	public static boolean compareEntities(List<? extends Entity> list, Entity ... entities) {
		if (list.size() != entities.length) {
			return false;
		}
		Set<? extends Entity> set = new HashSet<>(list);
		for (Entity entity : entities) {
			if (!set.contains(entity)) {
				return false;
			}
		}
		return true;
	}

}
