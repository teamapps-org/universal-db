/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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

import org.teamapps.universaldb.index.DataBaseMapper;
import org.teamapps.universaldb.cluster.network.MessageType;
import org.teamapps.universaldb.transaction.TransactionPacket;
import org.teamapps.universaldb.transaction.TransactionRequest;

import java.io.IOException;

public class ResolvedTransactionRequest implements ClusterMessage {

	private final TransactionRequest transactionRequest;
	private final byte[] data;

	public ResolvedTransactionRequest(TransactionRequest transactionRequest) throws IOException {
		this.transactionRequest = transactionRequest;
		this.data = transactionRequest.getPacket().writePacketBytes();
	}

	public ResolvedTransactionRequest(byte[] data, DataBaseMapper dataBaseMapper) throws IOException {
		this.data = data;
		TransactionPacket transactionPacket = new TransactionPacket(data);
		this.transactionRequest = new TransactionRequest(transactionPacket, dataBaseMapper);
	}

	public TransactionRequest getTransactionRequest() {
		return transactionRequest;
	}

	@Override
	public MessageType getType() {
		return MessageType.RESOLVED_TRANSACTION;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
