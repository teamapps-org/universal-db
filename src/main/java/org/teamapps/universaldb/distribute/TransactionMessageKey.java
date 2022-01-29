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
package org.teamapps.universaldb.distribute;

import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionMessageKey {

	public static TransactionMessageKey createFromKey(TransactionMessageKey key, long packetKey, String masterClientId, long masterOffset) {
		return new TransactionMessageKey(key.getMessageType(), key.getClientId(), packetKey, key.getTransactionKeyOfCallingNode(), masterClientId, masterOffset);
	}

	private static AtomicLong localTransactionKeyGenerator = new AtomicLong();

	private final TransactionMessageType messageType;
	private final String clientId;
	private final long packetKey;
	private final long transactionKeyOfCallingNode;
	private String masterClientId;
	private long masterOffset;

	public TransactionMessageKey(TransactionMessageType messageType, String clientId, long packetKey) {
		this.messageType = messageType;
		this.clientId = clientId;
		this.packetKey = packetKey;
		this.transactionKeyOfCallingNode = localTransactionKeyGenerator.incrementAndGet();
	}

	public TransactionMessageKey(TransactionMessageType messageType, String clientId, long packetKey, long transactionKeyOfCallingNode, String masterClientId, long masterOffset) {
		this.messageType = messageType;
		this.clientId = clientId;
		this.packetKey = packetKey;
		this.transactionKeyOfCallingNode = transactionKeyOfCallingNode;
		this.masterClientId = masterClientId;
		this.masterOffset = masterOffset;
	}

	public TransactionMessageKey(byte[] bytes) throws IOException {
		DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
		messageType = TransactionMessageType.values()[dataInputStream.readInt()];
		transactionKeyOfCallingNode = dataInputStream.readLong();
		clientId = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		packetKey = dataInputStream.readLong();
		masterClientId = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		masterOffset = dataInputStream.readLong();
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
		dataOutputStream.writeInt(messageType.ordinal());
		dataOutputStream.writeLong(transactionKeyOfCallingNode);
		DataStreamUtil.writeStringWithLengthHeader(dataOutputStream, clientId);
		dataOutputStream.writeLong(packetKey);
		DataStreamUtil.writeStringWithLengthHeader(dataOutputStream, masterClientId);
		dataOutputStream.writeLong(masterOffset);
		return byteArrayOutputStream.toByteArray();
	}

	public TransactionMessageType getMessageType() {
		return messageType;
	}

	public String getClientId() {
		return clientId;
	}

	public long getPacketKey() {
		return packetKey;
	}

	public String getMasterClientId() {
		return masterClientId;
	}

	public long getMasterOffset() {
		return masterOffset;
	}

	public long getTransactionKeyOfCallingNode() {
		return transactionKeyOfCallingNode;
	}

	@Override
	public String toString() {
		return "TransactionMessageKey{" +
				"messageType=" + messageType +
				", clientId='" + clientId + '\'' +
				", packetKey=" + packetKey +
				", transactionKeyOfCallingNode=" + transactionKeyOfCallingNode +
				", masterClientId='" + masterClientId + '\'' +
				", masterOffset=" + masterOffset +
				'}';
	}
}
