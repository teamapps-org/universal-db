package org.teamapps.universaldb.index.log;

import org.junit.Test;
import org.teamapps.cluster.protocol.NodeInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.*;

public class ChunkedIndexMessageStoreTest {

	@Test
	public void addMessage() throws IOException {
		File path = createTempDir();
		ChunkedIndexMessageStore<NodeInfo> store = createMessageStore(path);
		int size = 300;
		long pos = 0;
		for (int i = 0; i < size; i++) {
			pos = store.addMessage(new NodeInfo().setPort(i).setHost("" + i));
		}
		pos = store.addMessage(new NodeInfo().setNodeId("1").setHost("test"));
		NodeInfo nodeInfo = store.readMessage(pos);
		assertEquals("1", nodeInfo.getNodeId());
		assertEquals("test", nodeInfo.getHost());

	}

	@Test
	public void readMessage() {
	}

	@Test
	public void readLastChunks() throws IOException {
		File path = createTempDir();
		ChunkedIndexMessageStore<NodeInfo> store = createMessageStore(path);
		int size = 300;
		for (int i = 0; i < size; i++) {
			store.addMessage(new NodeInfo().setPort(i).setHost("" + i));
		}
		List<NodeInfo> nodeInfos = store.readLastMessages(1);
		assertTrue(nodeInfos.size() > 0);

		store.close();
		store = createMessageStore(path);
		nodeInfos = store.readLastMessages(1);
		assertTrue(nodeInfos.size() > 0);
	}

	private static File createTempDir() throws IOException {
		return Files.createTempDirectory("temp").toFile();
	}

	private static ChunkedIndexMessageStore<NodeInfo> createMessageStore(File path) throws IOException {
		return new ChunkedIndexMessageStore<>(path, "chunk-index-text", 100, false, false, NodeInfo.getMessageDecoder());
	}
}