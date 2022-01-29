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
package org.teamapps.universaldb.cluster.message;

import org.teamapps.universaldb.cluster.network.MessageType;

import java.nio.ByteBuffer;

public class HeadElectionProposal implements ClusterMessage {

	private final int headId;
	private final byte[] data;

	public HeadElectionProposal(int headId) {
		this.headId = headId;
		this.data = new byte[4];
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.putInt(headId);
	}

	public HeadElectionProposal(byte[] data) {
		this.data = data;
		ByteBuffer buffer = ByteBuffer.wrap(data);
		headId = buffer.getInt();
	}

	public int getHeadId() {
		return headId;
	}

	@Override
	public MessageType getType() {
		return MessageType.HEAD_ELECTION_PROPOSAL;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
