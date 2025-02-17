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
package org.teamapps.universaldb.message;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teamapps.test.protocol.Company;
import org.teamapps.test.protocol.Employee;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.*;

public class MessageStoreTest {

	private static File TEMP_DIR;

	@BeforeClass
	public static void setUp() throws Exception {
		TEMP_DIR = Files.createTempDirectory("temp").toFile().getParentFile();
	}



	@Test
	public void save() throws IOException {
		MessageStore<Company> messageStore = createStore(createRandomStoreName(), null);
		int size = 1_000;
		for (int i = 0; i < size; i++) {
			Company company = new Company().setName("test-" + i).setCeo(new Employee().setLastName("n" + i));
			messageStore.save(company);
		}
		assertEquals(size, messageStore.getMessageCount());
		List<Company> allMessages = messageStore.getAllMessages();
		assertEquals(size, allMessages.size());
		assertEquals("n999", allMessages.get(999).getCeo().getLastName());
		for (int i = 0; i < size; i++) {
			Company msg = messageStore.getById(i + 1);
			assertEquals("test-" + i, msg.getName());
		}
		List<Company> previousMessages = messageStore.getPreviousMessages(900, 3);
		assertEquals(3, previousMessages.size());
		assertEquals("test-" + 897, previousMessages.get(0).getName());
		assertEquals("test-" + 899, previousMessages.get(2).getName());
		assertEquals(900, previousMessages.get(2).getRecordId());
	}

	@Test
	public void delete() throws IOException {
		MessageStore<Company> messageStore = createStore(createRandomStoreName(), null);
		int size = 10;
		for (int i = 0; i < size; i++) {
			Company company = new Company().setName("test-" + i).setCeo(new Employee().setLastName("n" + i));
			messageStore.save(company);
		}
		assertEquals(size, messageStore.getMessageCount());
		List<Company> allMessages = messageStore.getAllMessages();
		assertEquals(size, allMessages.size());

		messageStore.delete(1);
		messageStore.delete(10);
		messageStore.delete(5);

		assertEquals(size - 3, messageStore.getMessageCount());
		allMessages = messageStore.getAllMessages();
		assertEquals(size - 3, allMessages.size());
		assertEquals(3,messageStore.getDeletedCount());

		List<Company> previousMessages = messageStore.getPreviousMessages(6, 3);
		assertEquals(3, previousMessages.get(0).getRecordId());
		assertEquals(4, previousMessages.get(1).getRecordId());
		assertEquals(6, previousMessages.get(2).getRecordId());

		messageStore.undelete(5);
		assertEquals(size - 2, messageStore.getMessageCount());
		allMessages = messageStore.getAllMessages();
		assertEquals(size - 2, allMessages.size());
		assertEquals(2,messageStore.getDeletedCount());
	}

	@Test
	public void undelete() {
	}

	@Test
	public void getById() {
	}

	@Test
	public void getByPosition() {
	}

	@Test
	public void getAllMessages() {
	}

	@Test
	public void getPreviousMessages() {
	}

	@Test
	public void getNextMessages() {
	}

	@Test
	public void getMessageVersions() throws IOException {
		MessageStore<Company> messageStore = createStore(createRandomStoreName(), null);
		int size = 10;
		for (int i = 0; i < size; i++) {
			Company company = new Company().setName("test-" + i).setCeo(new Employee().setLastName("n" + i));
			messageStore.save(company);
		}
		assertEquals(size, messageStore.getMessageCount());
		List<Company> allMessages = messageStore.getAllMessages();
		assertEquals(size, allMessages.size());

		Company company = messageStore.getById(3);
		company.setCeo(new Employee().setFirstName("John").setLastName("Smith"));
		messageStore.save(company);
		assertEquals("Smith", messageStore.getById(3).getCeo().getLastName());
		List<Company> messageVersions = messageStore.getMessageVersions(3);
		assertEquals(2, messageVersions.size());
		assertEquals("n2", messageVersions.get(1).getCeo().getLastName());
		company.setType("new type");
		messageStore.save(company);
		assertEquals(3, messageStore.getMessageVersions(3).size());
		assertEquals(1, messageStore.getMessageVersions(2).size());
	}

	@Test
	public void iterate() {
	}

	@Test
	public void iterateDeleted() {
	}

	@Test
	public void getStream() {
	}

	@Test
	public void isEmpty() {
	}

	@Test
	public void getStoreSize() {
	}

	private static String createRandomStoreName() {
		return "temp-store-" + ((long) (System.currentTimeMillis() * Math.random() * 1_000));
	}

	private static MessageStore<Company> createStore(String storeName, MessageCache cache) throws IOException {
		return new MessageStoreImpl<>(TEMP_DIR, storeName, Company.getMessageDecoder(), cache);
	}
}
