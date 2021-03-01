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
package org.teamapps.universaldb;

import com.google.common.io.Files;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teamapps.s3model.SchemaInfo;
import org.teamapps.s3model.s3db.S3Table;
import org.teamapps.universaldb.index.file.CachingS3FileStore;
import org.teamapps.universaldb.index.file.FileValue;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore
public class S3Test {

	private final static String S3_URL = "https://play.min.io:9000";
	private final static String S3_KEY = "Q3AM3UQ867SPQQA43P2F";
	private final static String S3_SECRET = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG";


	private static CachingS3FileStore fileStore;

	@BeforeClass
	public static void init() throws Exception {
		File tempDir = Files.createTempDir();
		fileStore = new CachingS3FileStore(tempDir, "test-bucket", S3_URL, S3_KEY, S3_SECRET);
		UniversalDB.createStandalone(tempDir, new SchemaInfo(), fileStore, true);
	}


	@Test
	public void testS3() throws IOException {
		fileStore.setCachingActive(true);
		File tempFile = TestBase.createResourceFile();
		S3Table s3Table = S3Table.create();
		s3Table
				.setIntField(1)
				.setFileField(tempFile)
				.setTextField("test file")
				.save();

	}

	@Test
	public void testS3NonCaching() throws IOException {
		fileStore.setCachingActive(false);
		File tempFile = TestBase.createResourceFile();

		S3Table s3Table = S3Table.create();
		s3Table
				.setIntField(1)
				.setFileField(tempFile)
				.setTextField("test file")
				.save();

		FileValue fileField = s3Table.getFileField();
		File file = fileField.retrieveFile();
		assertEquals(tempFile.length(), file.length());
	}
}
