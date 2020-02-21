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


public class ConnectToHeadWaitResponse implements ClusterMessage {

	private final int waitTime;
	private final byte[] data;

	public ConnectToHeadWaitResponse(int waitTime) {
		this.waitTime = waitTime;
		this.data = new byte[4];
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.putInt(waitTime);
	}

	public ConnectToHeadWaitResponse(byte[] data) {
		this.data = data;
		ByteBuffer buffer = ByteBuffer.wrap(data);
		waitTime = buffer.getInt();
	}

	public int getint() {
		return waitTime;
	}

	@Override
	public MessageType getType() {
		return MessageType.CONNECT_TO_HEAD_WAIT_RESPONSE;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
