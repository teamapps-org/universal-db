/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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
	private DatabaseModel mergedModel;


	public ModelUpdate(DatabaseModel databaseModel, long transactionId, long timestamp) {
		this.databaseModel = databaseModel;
		this.timestamp = timestamp;
		this.transactionId = transactionId;
	}

	public ModelUpdate(byte[] data) {
		this(new DataInputStream(new ByteArrayInputStream(data)));
	}

	public ModelUpdate(DataInputStream dis) {
		try {
			databaseModel = new DatabaseModel(dis);
			timestamp = dis.readLong();
			transactionId = dis.readLong();
			if (dis.readBoolean()) {
				mergedModel = new DatabaseModel(dis);
			}
		} catch (IOException e) {
			throw new RuntimeException("Error reading log update", e);
		}
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		write(dos);
		return bos.toByteArray();
	}

	public void write(DataOutputStream dos) throws IOException {
		databaseModel.write(dos);
		dos.writeLong(timestamp);
		dos.writeLong(transactionId);
		if (mergedModel == null) {
			dos.writeBoolean(false);
		} else {
			dos.writeBoolean(true);
			mergedModel.write(dos);
		}
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

	public DatabaseModel getMergedModel() {
		return mergedModel;
	}

	public void setMergedModel(DatabaseModel mergedModel) {
		this.mergedModel = mergedModel;
	}
}
