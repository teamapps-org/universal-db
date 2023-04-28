/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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
package org.teamapps.universaldb.index.transaction.schema;

import org.teamapps.universaldb.model.DatabaseModel;

import java.io.*;

public class ModelUpdate {
	private final DatabaseModel databaseModel;
	private final long timestamp;
	private final long transactionId;


	public ModelUpdate(DatabaseModel databaseModel, long transactionId, long timestamp) {
		this.databaseModel = databaseModel;
		this.timestamp = timestamp;
		this.transactionId = transactionId;
	}

	public ModelUpdate(byte[] data) {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
		try {
			databaseModel = new DatabaseModel(dis);
			timestamp = dis.readLong();
			transactionId = dis.readLong();
		} catch (IOException e) {
			throw new RuntimeException("Error reading log update", e);
		}
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		databaseModel.write(dos);
		dos.writeLong(timestamp);
		dos.writeLong(transactionId);
		return bos.toByteArray();
	}

	public DatabaseModel getDatabaseModel() {
		return databaseModel;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getTransactionId() {
		return transactionId;
	}
}
