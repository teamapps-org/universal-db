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

import org.agrona.concurrent.AtomicBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.transaction.TransactionIdHandler;
import org.teamapps.universaldb.util.MappedStoreUtil;

import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class SchemaStats implements TransactionIdHandler {

	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private static final int FIRST_SYSTEM_START_TIMESTAMP_POS = 0;
	private static final int SYSTEM_START_TIMESTAMP_POS = 8;
	private static final int SHUTDOWN_TIMESTAMP_POS = 16;
	private static final int LAST_TRANSACTION_ID_POS = 24;
	private static final int LAST_TRANSACTION_TIMESTAMP_POS = 32;
	private static final int START_UP_COUNT_POS = 40;
	private static final int TRANSACTION_OFFSET_POS = 48;
	private static final int MASTER_TRANSACTION_OFFSET_POS = 56;


	private static final int CLIENT_ID_POS = 100;
	private static final int GROUP_ID_POS = 200;
	private static final int MASTER_CLIENT_ID_POS = 300;
	private static final int MASTER_GROUP_ID_POS = 400;
	private static final int SCHEMA_POS = 500;

	private final File path;
	private AtomicBuffer buffer;
	private Schema schema;
	private final String clientId;
	private final String groupId;
	private final String masterClientId;
	private final String masterGroupId;
	private long startupCount;

	public SchemaStats(File path) {
		this.path = path;
		File file = new File(this.path, "database.stat");
		buffer = MappedStoreUtil.createAtomicBuffer(file, 500_000);
		if (readLong(FIRST_SYSTEM_START_TIMESTAMP_POS) == 0) {
			writeLong(FIRST_SYSTEM_START_TIMESTAMP_POS, System.currentTimeMillis());
		}
		writeLong(SYSTEM_START_TIMESTAMP_POS, System.currentTimeMillis());
		if (readInt(CLIENT_ID_POS) == 0) {
			clientId = UUID.randomUUID().toString().replace("-", "");
			groupId = UUID.randomUUID().toString().replace("-", "");
			masterClientId = UUID.randomUUID().toString().replace("-", "");
			masterGroupId = UUID.randomUUID().toString().replace("-", "");
			writeString(CLIENT_ID_POS, clientId);
			writeString(GROUP_ID_POS, groupId);
			writeString(MASTER_CLIENT_ID_POS, masterClientId);
			writeString(MASTER_GROUP_ID_POS, masterGroupId);
		} else {
			clientId = readString(CLIENT_ID_POS);
			groupId = readString(GROUP_ID_POS);
			masterClientId = readString(MASTER_CLIENT_ID_POS);
			masterGroupId = readString(MASTER_GROUP_ID_POS);
		}
		startupCount = readLong(START_UP_COUNT_POS) + 1;
		writeLong(START_UP_COUNT_POS, startupCount);
	}

	public Schema loadSchema() throws IOException {
		int len = buffer.getInt(SCHEMA_POS);
		if (len == 0) {
			return null;
		}
		byte[] bytes = new byte[len];
		buffer.getBytes(SCHEMA_POS + 4, bytes);
		return new Schema(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	public void saveSchema(Schema schema) throws IOException {
		byte[] bytes = schema.getSchemaData();
		buffer.putInt(SCHEMA_POS, bytes.length);
		buffer.putBytes(SCHEMA_POS + 4, bytes);
		this.schema = schema;
	}

	public Schema getSchema() {
		return schema;
	}

	public String getClientId() {
		return clientId;
	}

	public String getGroupId() {
		return groupId;
	}

	public String getMasterClientId() {
		return masterClientId;
	}

	public String getMasterGroupId() {
		return masterGroupId;
	}

	public long getStartupCount() {
		return startupCount;
	}

	public long getFirstSystemStartTimestamp() {
		return readLong(FIRST_SYSTEM_START_TIMESTAMP_POS);
	}

	public long getLastTransactionId() {
		return readLong(LAST_TRANSACTION_ID_POS);
	}

	@Override
	public long getAndCommitNextTransactionId() {
		long transactionId = getLastTransactionId();
		transactionId++;
		writeLong(LAST_TRANSACTION_ID_POS, transactionId);
		writeLong(LAST_TRANSACTION_TIMESTAMP_POS, System.currentTimeMillis());
		return transactionId;
	}

	@Override
	public void commitTransactionId(long transactionId) {
		if (getLastTransactionId() + 1 != transactionId) {
			logger.error("Wrong transaction id to commit, expected:" + (getLastTransactionId() + 1) + ", actual:" + transactionId);
		}
		writeLong(LAST_TRANSACTION_ID_POS, transactionId);
	}

	public long getLastTransactionTimestamp() {
		return readLong(LAST_TRANSACTION_TIMESTAMP_POS);
	}

	public long getTransactionOffset() {
		return readLong(TRANSACTION_OFFSET_POS);
	}

	public void setTransactionOffset(long transactionOffset) {
		writeLong(TRANSACTION_OFFSET_POS, transactionOffset);
	}

	public long getMasterTransactionOffset() {
		return readLong(MASTER_TRANSACTION_OFFSET_POS);
	}

	public void setMasterTransactionOffset(long masterTransactionOffset) {
		writeLong(MASTER_TRANSACTION_OFFSET_POS, masterTransactionOffset);
	}

	private long readLong(int pos) {
		return buffer.getLongVolatile(pos);
	}

	private void writeLong(int pos, long value) {
		buffer.putLongVolatile(pos, value);
	}

	private int readInt(int pos) {
		return buffer.getIntVolatile(pos);
	}

	private void writeInt(int pos, int value) {
		buffer.putIntVolatile(pos, value);
	}

	private String readString(int pos) {
		int length = buffer.getIntVolatile(pos);
		byte[] bytes = new byte[length];
		buffer.getBytes(pos + 4, bytes);
		return new String(bytes, StandardCharsets.UTF_8);
	}

	private void writeString(int pos, String s) {
		byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
		buffer.putIntVolatile(pos, bytes.length);
		buffer.putBytes(pos + 4, bytes);
	}


	@Override
	public long getLastCommittedTransactionId() {
		return getLastTransactionId();
	}




}
