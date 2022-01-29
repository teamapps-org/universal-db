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
package org.teamapps.universaldb.transaction;

import org.teamapps.universaldb.index.DataBaseMapper;

import java.io.IOException;

public class TransactionRequest {

	private final ClusterTransaction transaction;
	private TransactionPacket packet;
	private volatile boolean isExecuted;

	public TransactionRequest(ClusterTransaction transaction) {
		this.transaction = transaction;
	}

	public TransactionRequest(TransactionPacket packet, DataBaseMapper dataBaseMapper) throws IOException {
		this.packet = packet;
		this.transaction = new ClusterTransaction(packet, dataBaseMapper);
	}

	public TransactionPacket getPacket() throws IOException {
		if (packet == null) {
			packet = transaction.createTransactionPacket();
		}
		return packet;
	}

	public ClusterTransaction getTransaction() {
		return transaction;
	}

	public boolean isExecuted() {
		return transaction.getTransactionId() != 0;
	}

	public void executeUnresolvedTransaction(TransactionIdHandler transactionIdHandler) throws IOException {
		this.packet = transaction.resolveAndExecuteTransaction(transactionIdHandler, packet);
	}

	public void executeResolvedTransaction(TransactionIdHandler transactionIdHandler) {
		transaction.executeResolvedTransaction(transactionIdHandler);
	}

	public synchronized void waitForExecution() {
		while (!isExecuted) {
			try {
				wait();
			} catch (InterruptedException ignore) { }
		}
	}

	public synchronized void setExecuted() {
		isExecuted = true;
		notifyAll();
	}




}
