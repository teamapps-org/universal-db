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
package org.teamapps.universaldb.cluster.message;

import org.teamapps.universaldb.cluster.network.MessageType;

import java.nio.ByteBuffer;

public class ConnectToHeadSuccessResponse implements ClusterMessage {

	private final long lastTransactionId;
	private final long currentTransactionId;
	private final long transactionCount;
	private final byte[] data;

	public ConnectToHeadSuccessResponse(long lastTransactionId, long currentTransactionId, long transactionCount) {
		this.lastTransactionId = lastTransactionId;
		this.currentTransactionId = currentTransactionId;
		this.transactionCount = transactionCount;
		this.data = new byte[40];
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.putLong(lastTransactionId);
		buffer.putLong(currentTransactionId);
		buffer.putLong(transactionCount);
	}

	public ConnectToHeadSuccessResponse(byte[] data) {
		this.data = data;
		ByteBuffer buffer = ByteBuffer.wrap(data);
		lastTransactionId = buffer.getLong();
		currentTransactionId = buffer.getLong();
		transactionCount = buffer.getLong();
	}

	public long getLastTransactionId() {
		return lastTransactionId;
	}

	public long getCurrentTransactionId() {
		return currentTransactionId;
	}

	public long getTransactionCount() {
		return transactionCount;
	}

	@Override
	public MessageType getType() {
		return MessageType.CONNECT_TO_HEAD_SUCCESS_RESPONSE;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
