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
package org.teamapps.universaldb.transaction;

import java.io.*;

public class TransactionPacket {

	private final long transactionRequestId;
	private final long timestamp;
	private final int userId;
	private byte[] transactionData;
	private long transactionId;
	private byte[] correlationData;

	public TransactionPacket(long transactionRequestId, long transactionId, long timestamp, int userId) {
		this.transactionRequestId = transactionRequestId;
		this.transactionId = transactionId;
		this.timestamp = timestamp;
		this.userId = userId;
	}

	public TransactionPacket(byte[] bytes) throws IOException {
		this(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	public TransactionPacket(DataInputStream dataInputStream) throws IOException {
		this.transactionRequestId = dataInputStream.readLong();
		this.transactionId = dataInputStream.readLong();
		this.timestamp = dataInputStream.readLong();
		this.userId = dataInputStream.readInt();
		int transactionDataLength = dataInputStream.readInt();
		transactionData = new byte[transactionDataLength];
		dataInputStream.readFully(transactionData);
		int correlationDataLength = dataInputStream.readInt();
		if (correlationDataLength > 0) {
			correlationData = new byte[correlationDataLength];
			dataInputStream.readFully(correlationData);
		}
	}

	public long getTransactionRequestId() {
		return transactionRequestId;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public int getUserId() {
		return userId;
	}

	public byte[] getTransactionData() {
		return transactionData;
	}

	public void setTransactionId(long transactionId) {
		this.transactionId = transactionId;
	}

	public void setTransactionData(byte[] transactionData) {
		this.transactionData = transactionData;
	}

	public byte[] getCorrelationData() {
		return correlationData;
	}

	public void setCorrelationData(byte[] correlationData) {
		this.correlationData = correlationData;
	}

	public int getPacketLengthWithAllHeaders() {
		int length = 24 + transactionData.length;
		if (correlationData != null) {
			length += correlationData.length;
		}
		return length;
	}

	public void writePacket(DataOutputStream outputStream) throws IOException {
		outputStream.writeLong(transactionRequestId);
		outputStream.writeLong(transactionId);
		outputStream.writeLong(timestamp);
		outputStream.writeInt(userId);
		outputStream.writeInt(transactionData.length);
		outputStream.write(transactionData);
		if (correlationData != null) {
			outputStream.writeInt(correlationData.length);
			outputStream.write(correlationData);
		} else {
			outputStream.writeInt(0);
		}
	}

	public byte[] writePacketBytes() throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(getPacketLengthWithAllHeaders() + 4);
		writePacket(new DataOutputStream(byteArrayOutputStream));
		return byteArrayOutputStream.toByteArray();
	}
}
