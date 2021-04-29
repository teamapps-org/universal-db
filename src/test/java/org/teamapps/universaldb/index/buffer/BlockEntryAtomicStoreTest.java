package org.teamapps.universaldb.index.buffer;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class BlockEntryAtomicStoreTest {

	private static BlockEntryAtomicStore store;

	@BeforeClass
	public static void setup() {
		File tempDir = Files.createTempDir();
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
		for (int id = 1; id < 1_000; id++) {
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