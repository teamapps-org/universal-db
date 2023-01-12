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

import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.test.protocol.TestMessage;
import org.teamapps.test.protocol.TestUser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class MessageStoreTest {

	private static File TEMP_DIR;

	@BeforeClass
	public static void setUp() throws Exception {
		TEMP_DIR = Files.createTempDirectory("temp").toFile().getParentFile();
	}

	@Test
	public void getMessageCount() throws IOException {
		String name = createRandomStoreName();
		MessageStore<TestMessage> store = createStore(name);
		assertEquals(0, store.getMessageCount());
		int size = 10;
		for (int i = 0; i < size; i++) {
			store.saveMessage(new TestMessage().setTestId(i));
		}
		assertEquals(10, store.getMessageCount());
		store.close();
		store = createStore(name);
		assertEquals(10, store.getMessageCount());
		for (int i = 0; i < size; i++) {
			assertEquals(i, store.readAllMessages().get(i).getTestId());
		}
	}

	@Test
	public void addMessage() throws IOException {
		String name = createRandomStoreName();
		MessageStore<TestMessage> store = createStore(name);
		int size = 10_000;
		for (int i = 0; i < size; i++) {
			store.saveMessage(new TestMessage().setTestId(i).setSubject("test-" + i).setAuthor(new TestUser().setTestId(i).setLastName("last-" + i)));
		}
		List<TestMessage> messages = store.readAllMessages();
		for (int i = 0; i < size; i++) {
			assertEquals(i, messages.get(i).getTestId());
		}
		int partSize = 1_000;
		List<TestMessage> testMessages = store.readLastMessages(partSize);
		assertEquals(partSize, testMessages.size());
		assertEquals(9_001, testMessages.get(0).getRecordId());
		assertEquals(9_000, testMessages.get(0).getTestId());

		for (int i = 0; i < partSize; i++) {
			assertEquals(i + 9_001, testMessages.get(i).getRecordId());
		}
		store.close();
		store = createStore(name);

		messages = store.readAllMessages();
		for (int i = 0; i < size; i++) {
			assertEquals(i, messages.get(i).getTestId());
		}
	}

	@Test
	public void updateMessage() throws IOException {
		String name = createRandomStoreName();
		MessageStore<TestMessage> store = createStore(name);
		TestMessage msg1 = new TestMessage().setSubject("test 1");
		TestMessage msg2 = new TestMessage().setSubject("test 2");
		TestMessage msg3 = new TestMessage().setSubject("test 3");
		store.saveMessage(msg1);
		store.saveMessage(msg2);
		store.saveMessage(msg3);
		assertEquals(3, store.getMessageCount());
		assertEquals("test 2", store.readMessage(msg2.getRecordId()).getSubject());
		store.saveMessage(msg2.setSubject("test updated 2"));
		assertEquals("test updated 2", store.readMessage(msg2.getRecordId()).getSubject());
		store.close();
		store = createStore(name);
		assertEquals("test updated 2", store.readMessage(msg2.getRecordId()).getSubject());
	}

	@Test
	public void deleteMessage() throws IOException {
		String name = createRandomStoreName();
		MessageStore<TestMessage> store = createStore(name);
		TestMessage msg1 = new TestMessage().setSubject("test 1");
		TestMessage msg2 = new TestMessage().setSubject("test 2");
		TestMessage msg3 = new TestMessage().setSubject("test 3");
		store.saveMessage(msg1);
		store.saveMessage(msg2);
		store.saveMessage(msg3);
		assertEquals(3, store.getMessageCount());
		store.deleteMessage(msg2);
		assertEquals(2, store.getMessageCount());
		List<TestMessage> messages = store.readAllMessages();
		assertEquals(2, messages.size());
		assertEquals("test 1",messages.get(0).getSubject());
		assertEquals("test 3",messages.get(1).getSubject());

		store.readAllMessages().forEach(store::deleteMessage);
		assertEquals(0, store.getMessageCount());
		assertEquals(0, store.readAllMessages().size());
	}

	@Test
	public void undeleteMessage() throws IOException {
		String name = createRandomStoreName();
		MessageStore<TestMessage> store = createStore(name);
		TestMessage msg1 = new TestMessage().setSubject("test 1");
		TestMessage msg2 = new TestMessage().setSubject("test 2");
		TestMessage msg3 = new TestMessage().setSubject("test 3");
		store.saveMessage(msg1);
		store.saveMessage(msg2);
		store.saveMessage(msg3);
		assertEquals(3, store.getMessageCount());
		store.deleteMessage(msg1);
		store.deleteMessage(msg2);
		store.deleteMessage(msg3);
		assertEquals(0, store.getMessageCount());
		assertEquals(0, store.readAllMessages().size());
		store.undeleteMessage(msg3);
		store.undeleteMessage(msg1);
		List<TestMessage> messages = store.readAllMessages();
		assertEquals(2, messages.size());
		assertEquals("test 1",messages.get(0).getSubject());
		assertEquals("test 3",messages.get(1).getSubject());

		store.readAllMessages().forEach(store::deleteMessage);
		assertEquals(0, store.getMessageCount());
	}

	@Test
	public void readMessage() {
	}

	@Test
	public void readMessages() {

	}

	@Test
	public void readLastMessages() throws IOException {
		String name = createRandomStoreName();
		MessageStore<TestMessage> store = createStore(name);
		int size = 1_000;
		for (int i = 0; i < size; i++) {
			store.saveMessage(new TestMessage().setTestId(i).setSubject("test-" + i).setAuthor(new TestUser().setTestId(i).setLastName("last-" + i)));
		}
		for (int i = 0; i < size; i += 2) {
			TestMessage message = store.readMessage(i + 1);
			message.setBody("body-" + i);
			store.saveMessage(message);
		}
		store.deleteMessage(900);
		assertEquals(999, store.getMessageCount());
		List<TestMessage> messages = store.readLastMessages(10);
		assertEquals(991, messages.get(0).getRecordId());
		assertEquals(990, messages.get(0).getTestId());
		assertEquals("body-990", messages.get(0).getBody());
		assertEquals(992, messages.get(1).getRecordId());
		store.deleteMessage(991);
		messages = store.readLastMessages(10);
		assertEquals(990, messages.get(0).getRecordId());
	}

	@Test
	public void readAfterMessageId() {
	}

	@Test
	public void readBeforeMessageId() {
	}

	@Test
	public void readAllMessages() throws IOException {
		String name = createRandomStoreName();
		MessageStore<TestMessage> store = createStore(name);
		int size = 1_000;
		for (int i = 0; i < size; i++) {
			store.saveMessage(new TestMessage().setTestId(i).setSubject("test-" + i).setAuthor(new TestUser().setTestId(i).setLastName("last-" + i)));
		}
		List<TestMessage> messages = store.readAllMessages();
		for (int i = 0; i < size; i++) {
			assertEquals(i, messages.get(i).getTestId());
		}
	}

	private static String createRandomStoreName() {
		return "temp-store-" + ((long) (System.currentTimeMillis() * Math.random() * 1_000));
	}

	private static MessageStore<TestMessage> createStore(String storeName) throws IOException {
		return new MessageStore<>(TEMP_DIR, storeName, false, TestMessage.getMessageDecoder());
	}
}
