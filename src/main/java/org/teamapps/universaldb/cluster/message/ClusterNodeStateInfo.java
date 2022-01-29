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
package org.teamapps.universaldb.cluster.message;

import org.teamapps.universaldb.cluster.ClusterNodeRole;
import org.teamapps.universaldb.cluster.ClusterNodeState;
import org.teamapps.universaldb.cluster.network.MessageType;

import java.nio.ByteBuffer;

public class ClusterNodeStateInfo implements ClusterMessage {

	private final int nodeId;
	private final int headNodeId;
	private final ClusterNodeRole operatingRole;
	private final ClusterNodeRole preferredRole;
	private final ClusterNodeState nodeState;
	private final int connectedNodesCount;
	private final int clusterQuorum;
	private final int electoralClusterNodes;
	private final int syncingWithNodeId;
	private final long syncingTransactionStarted;
	private final long lastTransactionId;
	private final long currentTransactionId;
	private final long transactionCount;

	private final byte[] data;

	public ClusterNodeStateInfo(int nodeId, int headNodeId, ClusterNodeRole operatingRole, ClusterNodeRole preferredRole, ClusterNodeState nodeState, int connectedNodesCount, int clusterQuorum, int electoralClusterNodes, int syncingWithNodeId, long syncingTransactionStarted, long lastTransactionId, long currentTransactionId, long transactionCount) {
		this.nodeId = nodeId;
		this.headNodeId = headNodeId;
		this.operatingRole = operatingRole;
		this.preferredRole = preferredRole;
		this.nodeState = nodeState;
		this.connectedNodesCount = connectedNodesCount;
		this.clusterQuorum = clusterQuorum;
		this.electoralClusterNodes = electoralClusterNodes;
		this.syncingWithNodeId = syncingWithNodeId;
		this.syncingTransactionStarted = syncingTransactionStarted;
		this.lastTransactionId = lastTransactionId;
		this.currentTransactionId = currentTransactionId;
		this.transactionCount = transactionCount;
		this.data = new byte[59];
		ByteBuffer buffer = ByteBuffer.wrap(data);
		buffer.putInt(nodeId);
		buffer.putInt(headNodeId);
		buffer.put((byte) operatingRole.getId());
		buffer.put((byte) preferredRole.getId());
		buffer.put((byte) nodeState.getId());
		buffer.putInt(connectedNodesCount);
		buffer.putInt(clusterQuorum);
		buffer.putInt(electoralClusterNodes);
		buffer.putInt(syncingWithNodeId);
		buffer.putLong(syncingTransactionStarted);
		buffer.putLong(lastTransactionId);
		buffer.putLong(currentTransactionId);
		buffer.putLong(transactionCount);
	}


	public ClusterNodeStateInfo(byte[] data) {
		this.data = data;
		ByteBuffer buffer = ByteBuffer.wrap(data);
		nodeId = buffer.getInt();
		headNodeId = buffer.getInt();
		operatingRole = ClusterNodeRole.getById(buffer.get());
		preferredRole = ClusterNodeRole.getById(buffer.get());
		nodeState = ClusterNodeState.getById(buffer.get());
		connectedNodesCount = buffer.getInt();
		clusterQuorum = buffer.getInt();
		electoralClusterNodes = buffer.getInt();
		syncingWithNodeId = buffer.getInt();
		syncingTransactionStarted = buffer.getLong();
		lastTransactionId = buffer.getLong();
		currentTransactionId = buffer.getLong();
		transactionCount = buffer.getLong();
	}

	public int getNodeId() {
		return nodeId;
	}

	public int getHeadNodeId() {
		return headNodeId;
	}

	public ClusterNodeRole getOperatingRole() {
		return operatingRole;
	}

	public ClusterNodeRole getPreferredRole() {
		return preferredRole;
	}

	public ClusterNodeState getNodeState() {
		return nodeState;
	}

	public int getConnectedNodesCount() {
		return connectedNodesCount;
	}

	public int getClusterQuorum() {
		return clusterQuorum;
	}

	public int getElectoralClusterNodes() {
		return electoralClusterNodes;
	}

	public int getSyncingWithNodeId() {
		return syncingWithNodeId;
	}

	public long getSyncingTransactionStarted() {
		return syncingTransactionStarted;
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

	@Override
	public MessageType getType() {
		return MessageType.CLUSTER_NODE_STATUS_INFO;
	}

	@Override
	public byte[] getData() {
		return data;
	}
}
