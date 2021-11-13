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