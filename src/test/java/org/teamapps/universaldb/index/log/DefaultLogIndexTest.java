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
	public void writeLog() {
		Map<Integer, Long> positionMap = new HashMap<>();
		for (int i = 0; i < 1_000; i++) {
			long pos = logIndex.writeLog(TEST_DATA);
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
	public void writeLogCommitted() {
		Map<Integer, Long> positionMap = new HashMap<>();
		for (int i = 0; i < 1_000; i++) {
			long pos = logIndex.writeLogCommitted(TEST_DATA);
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
			long pos = logIndex.writeLogCommitted(TEST_DATA);
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
			long pos = logIndex.writeLogCommitted(testValue);
			positionMap.put(i, pos);
			byte[] bytes = logIndex.readLog(positionMap.get(i));
			assertArrayEquals(testValue, bytes);
		}
	}

	@Test
	public void getPosition() {
		DefaultLogIndex logIndex = new DefaultLogIndex(tempDir, "index-log-pos");
		assertEquals(4, logIndex.getPosition());
		logIndex.writeLogCommitted(TEST_DATA);
		assertEquals(4 + TEST_DATA.length + 4, logIndex.getPosition());
	}
}