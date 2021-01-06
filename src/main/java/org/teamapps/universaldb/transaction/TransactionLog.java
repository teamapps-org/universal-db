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

import java.util.Iterator;

public interface TransactionLog extends TransactionPacketWriter {


	long getLastCommittedTransactionId();

	/*
		Append bytes to transaction-log-file
		add entry in transaction-table: position in log file, transaction id
		update last committed transaction id
	 */
	//inherited: void writeTransaction(TransactionPacket transactionPacket);


	/*
		get positions from transaction-table
		read from log file
		while transactionId < last committed id
	 */
	Iterator<TransactionPacket> getTransactionPackets(long startTransactionId);

}
