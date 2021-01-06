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
import org.teamapps.universaldb.cluster.ClusterNodeRole;
import org.teamapps.universaldb.cluster.ClusterOperationMode;
import org.teamapps.universaldb.cluster.network.MessageType;

import java.io.IOException;
import java.nio.ByteBuffer;

public class InitMessage implements ClusterMessage {

	private final int nodeId;
	private final long lastTransactionId;
	private final long currentTransactionId;
	private final long transactionCount;
	private final Schema schema;
	private final ClusterOperationMode clusterOperationMode;
	private final ClusterNodeRole currentType;
	private final ClusterNodeRole preferredType;

	private final byte[] data;

	public InitMessage(int nodeId, long lastTransactionId, long currentTransactionId, long transactionCount, Schema schema, ClusterOperationMode clusterOperationMode, ClusterNodeRole currentType, ClusterNodeRole preferredType) throws IOException {
		this.nodeId = nodeId;
		this.lastTransactionId = lastTransactionId;
		this.currentTransactionId = currentTransactionId;
		this.transactionCount = transactionCount;
		this.schema = schema;
		this.clusterOperationMode = clusterOperationMode;
		this.currentType = currentType;
		this.preferredType = preferredType;
		byte[] schemaData = schema.getSchemaData();
		this.data = new byte[31 + schemaData.length + 4];
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.putInt(nodeId);
		buffer.putLong(lastTransactionId);
		buffer.putLong(currentTransactionId);
		buffer.putLong(transactionCount);
		ByteBufferUtil.putBytesWithHeader(schemaData, buffer);
		buffer.put((byte) clusterOperationMode.getId());
		buffer.put((byte) currentType.getId());
		buffer.put((byte) preferredType.getId());
	}

	public InitMessage(byte[] data) throws IOException {
		this.data = data;
		ByteBuffer buffer = ByteBuffer.wrap(data);
		nodeId = buffer.getInt();
		lastTransactionId = buffer.getLong();
		currentTransactionId = buffer.getLong();
		transactionCount = buffer.getLong();
		byte[] schemaBytes = ByteBufferUtil.getBytesWithHeader(buffer);
		schema = new Schema(schemaBytes);
		clusterOperationMode = ClusterOperationMode.getById(buffer.get());
		currentType = ClusterNodeRole.getById(buffer.get());
		preferredType = ClusterNodeRole.getById(buffer.get());
	}

	public int getNodeId() {
		return nodeId;
	}

	public Schema getSchema() {
		return schema;
	}

	public long getLastTransactionId() {
		return lastTransactionId;
	}

	public long getCurrentTransactionId() {
		return currentTransactionId;
	}

	public long getTransactionCount() {
		return transactionCount;
	}

	public ClusterOperationMode getClusterOperationMode() {
		return clusterOperationMode;
	}

	public ClusterNodeRole getCurrentType() {
		return currentType;
	}

	public ClusterNodeRole getPreferredType() {
		return preferredType;
	}

	@Override
	public MessageType getType() {
		return MessageType.INITIAL_MESSAGE;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
