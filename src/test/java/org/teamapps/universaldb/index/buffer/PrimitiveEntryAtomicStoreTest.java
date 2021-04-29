package org.teamapps.universaldb.index.buffer;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class PrimitiveEntryAtomicStoreTest {

	private static PrimitiveEntryAtomicStore store;

	@BeforeClass
	public static void setup() {
		File tempDir = Files.createTempDir();
		store = new PrimitiveEntryAtomicStore(tempDir, "primitiveEntryAtomicStoreTest");
	}

	@AfterClass
	public static void tearDown() {
		store.drop();
	}

	@Test
	public void getBoolean() {
		store.setBoolean(1, true);
		assertEquals(true, store.getBoolean(1));
	}

	@Test
	public void setBoolean() {
		store.setBoolean(1, true);
		assertEquals(true, store.getBoolean(1));
		store.setBoolean(1, false);
		assertEquals(false, store.getBoolean(1));
		for (int id = 1; id < 100_000; id++) {
			store.setBoolean(id, id % 3 == 0);
		}
		for (int id = 1; id < 100_000; id++) {
			assertEquals(id % 3 == 0, store.getBoolean(id));
		}
	}

	@Test
	public void getByte() {
		store.setByte(1, (byte) 1);
		assertEquals(1, store.getByte(1));
	}

	@Test
	public void setByte() {
		store.setByte(1, (byte) 1);
		assertEquals(1, store.getByte(1));
		store.setByte(1, (byte) 2);
		assertEquals(2, store.getByte(1));
	}

	@Test
	public void getShort() {
		store.setShort(1, (short) -1234);
		assertEquals(-1234, store.getShort(1));
	}

	@Test
	public void setShort() {
		for (int id = 1; id < 32_000; id++) {
			store.setShort(id, (short) id);
		}
		for (int id = 1; id < 32_000; id++) {
			assertEquals(id, store.getShort(id));
		}
	}

	@Test
	public void getInt() {
		store.setInt(1, Integer.MAX_VALUE);
		assertEquals(Integer.MAX_VALUE, store.getInt(1), 0.1d);
		store.setInt(1, Integer.MIN_VALUE);
		assertEquals(Integer.MIN_VALUE, store.getInt(1), 0.1d);
	}

	@Test
	public void setInt() {
		for (int id = 1; id < 100_000; id++) {
			store.setInt(id, id);
		}
		for (int id = 1; id < 100_000; id++) {
			assertEquals(id, store.getInt(id));
		}
	}

	@Test
	public void getFloat() {
		store.setFloat(1, Float.MAX_VALUE);
		assertEquals(Float.MAX_VALUE, store.getFloat(1), 0.1d);
		store.setFloat(1, Float.MIN_VALUE);
		assertEquals(Float.MIN_VALUE, store.getFloat(1), 0.1d);
	}

	@Test
	public void setFloat() {
		for (int id = 1; id < 100_000; id++) {
			store.setFloat(id, id);
		}
		for (int id = 1; id < 100_000; id++) {
			assertEquals(id, store.getFloat(id), 0.1d);
		}
	}

	@Test
	public void getLong() {
		store.setLong(1, Long.MAX_VALUE);
		assertEquals(Long.MAX_VALUE, store.getLong(1));
		store.setLong(1, Long.MIN_VALUE);
		assertEquals(Long.MIN_VALUE, store.getLong(1));
	}

	@Test
	public void setLong() {
		for (int id = 1; id < 100_000; id++) {
			store.setLong(id, id);
		}
		for (int id = 1; id < 100_000; id++) {
			assertEquals(id, store.getLong(id));
		}
	}

	@Test
	public void getDouble() {
		store.setDouble(1, Double.MAX_VALUE);
		assertEquals(Double.MAX_VALUE, store.getDouble(1), 0.1d);
		store.setDouble(1, Double.MIN_VALUE);
		assertEquals(Double.MIN_VALUE, store.getDouble(1), 0.1d);

	}

	@Test
	public void setDouble() {
		for (int id = 1; id < 100_000; id++) {
			store.setDouble(id, id);
		}
		for (int id = 1; id < 100_000; id++) {
			assertEquals(id, store.getDouble(id), 0.1d);
		}
	}
}