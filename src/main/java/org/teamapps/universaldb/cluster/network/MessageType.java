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
package org.teamapps.universaldb.cluster.network;

import java.util.HashMap;
import java.util.Map;

public enum MessageType {

	ENCRYPTION_INIT(1),
	ENCRYPTION_INIT_RESPONSE(2),
	INITIAL_MESSAGE(3),
	INITIAL_MESSAGE_RESPONSE(4),
	SCHEMA_UPDATE(5),
	SYNCHRONIZE_TRANSACTIONS_REQUEST(6),
	SYNCHRONIZE_TRANSACTIONS_RESPONSE(7),
	SYNCHRONIZE_TRANSACTIONS_FINISHED(8),
	SYNCHRONIZE_TRANSACTIONS_STATUS(9),
	HEAD_ELECTION_PROPOSAL(10),
	CONNECT_TO_HEAD_REQUEST(11),
	CONNECT_TO_HEAD_WAIT_RESPONSE(12),
	CONNECT_TO_HEAD_SUCCESS_RESPONSE(13),
	CLUSTER_NODE_STATUS_UPDATE(14),
	UNRESOLVED_TRANSACTION(15),
	RESOLVED_TRANSACTION(16),
	CLUSTER_NODE_STATUS_INFO(17),

	;

	private static Map<Integer, MessageType> messageTypeById = new HashMap<>();

	static {
		for (MessageType value : values()) {
			messageTypeById.put(value.getMessageId(), value);
		}

	}

	public static MessageType getById(int messageId) {
		return messageTypeById.get(messageId);
	}

	private final int messageId;

	MessageType(int messageId) {
		this.messageId = messageId;
	}

	public int getMessageId() {
		return messageId;
	}
}
