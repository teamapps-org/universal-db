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
package org.teamapps.universaldb.cluster.message;

import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.cluster.network.MessageType;

import java.io.IOException;

public class SchemaUpdateMessage implements ClusterMessage {

	private final Schema schema;
	private final byte[] data;

	public SchemaUpdateMessage(Schema schema) throws IOException {
		this.schema = schema;
		this.data = schema.getSchemaData();
	}

	public SchemaUpdateMessage(byte[] data) throws IOException {
		this.data = data;
		this.schema = new Schema(data);
	}

	public Schema getSchema() {
		return schema;
	}

	@Override
	public MessageType getType() {
		return MessageType.SCHEMA_UPDATE;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
