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
import org.teamapps.universaldb.schema.Schema;

import java.io.IOException;
import java.util.Iterator;

public interface TransactionHandler {

	long getCurrentTransactionId();

	long getLastTransactionId();

	long getTransactionCount();

	Schema getSchema();

	void updateSchema(Schema schema) throws IOException;

	Iterator<byte[]> getTransactions(long startTransaction, long lastTransaction);

	void handleTransactionSynchronizationPacket(TransactionPacket packet);

	DataBaseMapper getDatabaseMapper();

	void executeTransactionRequest(TransactionRequest transactionRequest) throws IOException;

}
