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
package org.teamapps.universaldb.transaction;

import org.teamapps.universaldb.UniversalDB;

import java.io.IOException;

public class Transaction {

	private static UniversalDB dataBase;

	public static void setDataBase(UniversalDB db) {
		dataBase = db;
	}

	public static Transaction create() {
		return new Transaction(UniversalDB.getUserId());
	}

	private final ClusterTransaction clusterTransaction;

	private Transaction(int userId) {
		this.clusterTransaction = new ClusterTransaction(userId);
	}

	public int getUserId() {
		return clusterTransaction.getUserId();
	}

	public void addTransactionRecord(TransactionRecord transactionRecord) {
		clusterTransaction.addTransactionRecord(transactionRecord);
	}

	public void execute() {
		try {
			dataBase.executeTransaction(clusterTransaction);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public int getResolvedRecordIdByCorrelationId(int correlationId) {
		return clusterTransaction.getResolvedRecordIdByCorrelationId(correlationId);
	}
}
