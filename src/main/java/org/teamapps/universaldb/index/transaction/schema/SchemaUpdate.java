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

import org.teamapps.universaldb.schema.Schema;

import java.io.*;

public class SchemaUpdate {
	private final Schema schema;
	private final long timestamp;
	private final long transactionId;


	public SchemaUpdate(Schema schema, long transactionId, long timestamp) {
		this.schema = schema;
		this.timestamp = timestamp;
		this.transactionId = transactionId;
	}

	public SchemaUpdate(byte[] data) {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
		try {
			schema = new Schema(dis);
			timestamp = dis.readLong();
			transactionId = dis.readLong();
		} catch (IOException e) {
			throw new RuntimeException("Error reading log update", e);
		}
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		schema.writeSchema(dos);
		dos.writeLong(timestamp);
		dos.writeLong(transactionId);
		return bos.toByteArray();
	}

	public Schema getSchema() {
		return schema;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getTransactionId() {
		return transactionId;
	}
}
