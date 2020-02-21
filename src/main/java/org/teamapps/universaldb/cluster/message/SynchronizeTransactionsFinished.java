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

public class SynchronizeTransactionsFinished implements ClusterMessage {

	private final long requestStartId;
	private final long requestEndId;
	private final byte[] data;

	public SynchronizeTransactionsFinished(long requestStartId, long requestEndId) {
		this.requestStartId = requestStartId;
		this.requestEndId = requestEndId;
		this.data = new byte[40];
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.putLong(requestStartId);
		buffer.putLong(requestEndId);
	}

	public SynchronizeTransactionsFinished(byte[] data) {
		this.data = data;
		ByteBuffer buffer = ByteBuffer.wrap(data);
		requestStartId = buffer.getLong();
		requestEndId = buffer.getLong();
	}

	public long getRequestStartId() {
		return requestStartId;
	}

	public long getRequestEndId() {
		return requestEndId;
	}

	@Override
	public MessageType getType() {
		return MessageType.SYNCHRONIZE_TRANSACTIONS_FINISHED;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
