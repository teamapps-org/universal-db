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
package org.teamapps.universaldb.cluster.message;

import org.teamapps.universaldb.cluster.ClusterNodeRole;
import org.teamapps.universaldb.cluster.ClusterOperationMode;
import org.teamapps.universaldb.cluster.network.MessageType;

import java.nio.ByteBuffer;

public class ClusterNodeStatusMessage implements ClusterMessage {

	private final int nodeId;
	private final long lastTransactionId;
	private final long currentTransactionId;
	private final long transactionCount;
	private final int transactionQueueSize;
	private final ClusterOperationMode clusterOperationMode;
	private final ClusterNodeRole currentType;
	private final ClusterNodeRole preferredType;
	private final int connectedNodes;
	private final int currentHead;

	private final byte[] data;

	public ClusterNodeStatusMessage(int nodeId,
	                                long lastTransactionId,
	                                long currentTransactionId,
	                                long transactionCount,
	                                int transactionQueueSize,
	                                ClusterOperationMode clusterOperationMode,
	                                ClusterNodeRole currentType,
	                                ClusterNodeRole preferredType,
	                                int connectedNodes,
	                                int currentHead) {
		this.nodeId = nodeId;
		this.lastTransactionId = lastTransactionId;
		this.currentTransactionId = currentTransactionId;
		this.transactionCount = transactionCount;
		this.transactionQueueSize = transactionQueueSize;
		this.clusterOperationMode = clusterOperationMode;
		this.currentType = currentType;
		this.preferredType = preferredType;
		this.connectedNodes = connectedNodes;
		this.currentHead = currentHead;
		this.data = new byte[43];
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.putInt(nodeId);
		buffer.putLong(lastTransactionId);
		buffer.putLong(currentTransactionId);
		buffer.putLong(transactionCount);
		buffer.putInt(transactionQueueSize);
		buffer.put((byte) clusterOperationMode.getId());
		buffer.put((byte) currentType.getId());
		buffer.put((byte) preferredType.getId());
		buffer.putInt(connectedNodes);
		buffer.putInt(currentHead);
	}

	public ClusterNodeStatusMessage(byte[] data) {
		this.data = data;
		ByteBuffer buffer = ByteBuffer.wrap(data);
		nodeId = buffer.getInt();
		lastTransactionId = buffer.getLong();
		currentTransactionId = buffer.getLong();
		transactionCount = buffer.getLong();
		transactionQueueSize = buffer.getInt();
		clusterOperationMode = ClusterOperationMode.getById(buffer.get());
		currentType = ClusterNodeRole.getById(buffer.get());
		preferredType = ClusterNodeRole.getById(buffer.get());
		connectedNodes = buffer.getInt();
		currentHead = buffer.getInt();
	}

	public int getNodeId() {
		return nodeId;
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

	public int getTransactionQueueSize() {
		return transactionQueueSize;
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

	public int getConnectedNodes() {
		return connectedNodes;
	}

	public int getCurrentHead() {
		return currentHead;
	}

	@Override
	public MessageType getType() {
		return MessageType.CLUSTER_NODE_STATUS_UPDATE;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
