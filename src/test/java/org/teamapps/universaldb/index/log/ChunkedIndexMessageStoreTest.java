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
		assertEquals(size, store.readLastMessages(1000).size());
	}

	private static File createTempDir() throws IOException {
		return Files.createTempDirectory("temp").toFile();
	}

	private static ChunkedIndexMessageStore<NodeInfo> createMessageStore(File path) throws IOException {
		return new ChunkedIndexMessageStore<>(path, "chunk-index-text", 100, false, false, NodeInfo.getMessageDecoder());
	}
}
