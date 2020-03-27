/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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
import java.util.Objects;

public class TransactionMessageKey {

	private final TransactionMessageType messageType;
	private final String clientId;
	private final long localKey;
	private String masterClientId;
	private long masterOffset;

	public TransactionMessageKey(TransactionMessageType messageType, String clientId, long localKey) {
		this.messageType = messageType;
		this.clientId = clientId;
		this.localKey = localKey;
	}

	public TransactionMessageKey(byte[] bytes) throws IOException {
		DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
		messageType = TransactionMessageType.values()[dataInputStream.readInt()];
		clientId = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		localKey = dataInputStream.readLong();
		masterClientId = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		masterOffset = dataInputStream.readLong();
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
		dataOutputStream.writeInt(messageType.ordinal());
		DataStreamUtil.writeStringWithLengthHeader(dataOutputStream, clientId);
		dataOutputStream.writeLong(localKey);
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

	public long getLocalKey() {
		return localKey;
	}

	public String getMasterClientId() {
		return masterClientId;
	}

	public void setMasterClientId(String masterClientId) {
		this.masterClientId = masterClientId;
	}

	public long getMasterOffset() {
		return masterOffset;
	}

	public void setMasterOffset(long masterOffset) {
		this.masterOffset = masterOffset;
	}

	@Override
	public String toString() {
		return "TransactionMessageKey{" +
				"messageType=" + messageType +
				", clientId='" + clientId + '\'' +
				", localKey=" + localKey +
				", masterClientId='" + masterClientId + '\'' +
				", masterOffset=" + masterOffset +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		TransactionMessageKey that = (TransactionMessageKey) o;
		//note: masterClientId and masterOffset may not be part of equals --> sender will compare against key without master day
		return localKey == that.localKey &&
				messageType == that.messageType &&
				clientId.equals(that.clientId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(messageType, clientId, localKey);
	}
}
