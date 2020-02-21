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
package org.teamapps.universaldb.cluster;

import org.teamapps.universaldb.cluster.message.*;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.cluster.network.ConnectionHandler;
import org.teamapps.universaldb.cluster.network.MessageType;
import org.teamapps.universaldb.cluster.network.NetworkWriter;
import org.teamapps.universaldb.transaction.TransactionHandler;
import org.teamapps.universaldb.transaction.TransactionRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClusterNode implements ConnectionHandler {


	private final ClusterNodeConfig nodeConfig;
	private final TransactionHandler transactionHandler;
	private final ClusterHandler clusterHandler;
	private final int nodeId;

	private NetworkWriter networkWriter;

	private volatile boolean connecting;
	private volatile boolean connected;

	private boolean initialized;
	private long lastTransactionId;
	private long currentTransactionId;
	private long transactionCount;
	private ClusterOperationMode clusterOperationMode;
	private ClusterNodeRole clusterNodeRole;
	private ClusterNodeRole preferredClusterNodeRole;
	private Schema schema;
	private int headIdProposal;

	private volatile boolean transactionSyncMode;
	private List<ResolvedTransactionRequest> resolvedTransactionRequests;
	private int maxTransactionRequestQueueSize = 1000;


	public ClusterNode(ClusterNodeConfig nodeConfig, TransactionHandler transactionHandler, ClusterHandler clusterHandler) {
		this.nodeConfig = nodeConfig;
		this.nodeId = nodeConfig.getNodeId();
		this.transactionHandler = transactionHandler;
		this.clusterHandler = clusterHandler;

		this.preferredClusterNodeRole = nodeConfig.getPreferredNodeType();
		this.resolvedTransactionRequests = new ArrayList<>();
	}

	public int getNodeId() {
		return nodeId;
	}

	public ClusterNodeConfig getConfig() {
		return nodeConfig;
	}

	public void setNetworkWriter(NetworkWriter networkWriter) {
		this.networkWriter = networkWriter;
		this.connecting = false;
		this.connected = true;
	}

	public boolean isConnected() {
		return connected;
	}

	public boolean isConnecting() {
		return connecting;
	}

	public void setConnecting(boolean connecting) {
		this.connecting = connecting;
	}

	public void handleResolvedTransactionMessage(ResolvedTransactionRequest transactionRequest) {
		if (transactionSyncMode) {
			resolvedTransactionRequests.add(transactionRequest);
			if (resolvedTransactionRequests.size() == maxTransactionRequestQueueSize) {
				//todo: close connection and restart -> or
			}
		} else {
			sendMessage(transactionRequest);
		}
	}

	public void sendSchemaUpdate(Schema schema) throws IOException {
		SchemaUpdateMessage schemaUpdateMessage = new SchemaUpdateMessage(schema);
		networkWriter.sendMessage(schemaUpdateMessage);
	}

	public void sendMessage(ClusterMessage message) {
		networkWriter.sendMessage(message);
	}

	@Override
	public void handleConnected(NetworkWriter networkWriter) {
		setNetworkWriter(networkWriter);
	}

	@Override
	public void handleConnectionError() {
		this.connected = false;
		this.connecting = false;
		clusterHandler.handleLostConnection(this);
	}

	@Override
	public void handleMessage(MessageType messageType, byte[] data, NetworkWriter networkWriter) {
		if (this.networkWriter == null) {
			setNetworkWriter(networkWriter);
		}
		switch (messageType) {
			case INITIAL_MESSAGE:
				handleInitialMessage(data);
				break;
			case INITIAL_MESSAGE_RESPONSE:
				handleInitialMessageResponse(data);
				break;
			case SCHEMA_UPDATE:
				handleSchemaUpdateMessage(data);
				break;
			case SYNCHRONIZE_TRANSACTIONS_REQUEST:
				handleSynchronizeTransactionRequest(new SynchronizeTransactionsRequest(data));
				break;
			case SYNCHRONIZE_TRANSACTIONS_RESPONSE:
				handleSynchronizeTransactionResponse(new SynchronizeTransactionResponse(data));
				break;
			case SYNCHRONIZE_TRANSACTIONS_FINISHED:
				handleSynchronizeTransactionsFinished(new SynchronizeTransactionsFinished(data));
				break;
			case SYNCHRONIZE_TRANSACTIONS_STATUS:
				handleSynchronizeTransactionsStatus(new SynchronizeTransactionsStatus(data));
				break;
			case HEAD_ELECTION_PROPOSAL:
				handleHeadElectionProposal(new HeadElectionProposal(data));
				break;
			case CONNECT_TO_HEAD_REQUEST:
				handleConnectToHeadRequest(new ConnectToHeadRequest(data));
				break;
			case CONNECT_TO_HEAD_WAIT_RESPONSE:
				handleConnectToHeadWaitResponse(new ConnectToHeadWaitResponse(data));
				break;
			case CONNECT_TO_HEAD_SUCCESS_RESPONSE:
				handleConnectToHeadSuccessResponse(new ConnectToHeadSuccessResponse(data));
				break;
			case CLUSTER_NODE_STATUS_UPDATE:
				handleClusterNodeStatusUpdate(new ClusterNodeStatusMessage(data));
				break;
			case UNRESOLVED_TRANSACTION:
				handleUnresolvedTransactionRequest(data);
				break;
			case RESOLVED_TRANSACTION:
				handleResolvedTransactionRequest(data);
				break;
		}
	}

	private void handleUnresolvedTransactionRequest(byte[] data) {
		try {
			UnresolvedTransactionRequest unresolvedTransactionRequest = new UnresolvedTransactionRequest(data, transactionHandler.getDatabaseMapper());
			TransactionRequest transactionRequest = unresolvedTransactionRequest.getTransactionRequest();
			clusterHandler.handleUnresolvedTransactionRequest(transactionRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void handleResolvedTransactionRequest(byte[] data) {
		try {
			ResolvedTransactionRequest resolvedTransactionRequest = new ResolvedTransactionRequest(data, transactionHandler.getDatabaseMapper());
			TransactionRequest transactionRequest = resolvedTransactionRequest.getTransactionRequest();
			clusterHandler.handleResolvedTransactionRequest(transactionRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void handleClusterNodeStatusUpdate(ClusterNodeStatusMessage clusterNodeStatusMessage) {
		lastTransactionId = clusterNodeStatusMessage.getLastTransactionId();
		currentTransactionId = clusterNodeStatusMessage.getCurrentTransactionId();
		transactionCount = clusterNodeStatusMessage.getTransactionCount();
		clusterOperationMode = clusterNodeStatusMessage.getClusterOperationMode();
		clusterNodeRole = clusterNodeStatusMessage.getCurrentType();
		preferredClusterNodeRole = clusterNodeStatusMessage.getPreferredType();
		clusterHandler.handleClusterNodeStatusUpdate(clusterNodeStatusMessage, this);
	}

	private void handleConnectToHeadSuccessResponse(ConnectToHeadSuccessResponse connectToHeadSuccessResponse) {
		clusterHandler.handleConnectToHeadSuccessResponse(connectToHeadSuccessResponse, this);
	}

	private void handleConnectToHeadWaitResponse(ConnectToHeadWaitResponse connectToHeadWaitResponse) {
		clusterHandler.handleConnectToHeadWaitResponse(connectToHeadWaitResponse, this);
	}

	private void handleConnectToHeadRequest(ConnectToHeadRequest connectToHeadRequest) {
		clusterHandler.handleConnectToHeadRequest(connectToHeadRequest, this);
	}

	private void handleHeadElectionProposal(HeadElectionProposal headElectionProposal) {
		this.headIdProposal = headElectionProposal.getHeadId();
		clusterHandler.handleHeadElectionProposal(headIdProposal, this);
	}

	private void handleSynchronizeTransactionsStatus(SynchronizeTransactionsStatus synchronizeTransactionsStatus) {
		lastTransactionId = synchronizeTransactionsStatus.getLastTransactionId();
		currentTransactionId = synchronizeTransactionsStatus.getCurrentTransactionId();
		transactionCount = synchronizeTransactionsStatus.getTransactionCount();

		clusterHandler.handleSynchronizeTransactionsStatus(synchronizeTransactionsStatus, this);
	}

	private void handleSynchronizeTransactionsFinished(SynchronizeTransactionsFinished synchronizeTransactionsFinished) {
		clusterHandler.handleSynchronizeTransactionsFinished(synchronizeTransactionsFinished, this);
	}

	private void handleSynchronizeTransactionResponse(SynchronizeTransactionResponse synchronizeTransactionResponse) {
		clusterHandler.handleSynchronizeTransactionResponse(synchronizeTransactionResponse, this);
	}

	private void handleSynchronizeTransactionRequest(SynchronizeTransactionsRequest message) {
		clusterHandler.handleSynchronizeTransactionRequest(message, networkWriter);
	}

	private void handleSchemaUpdateMessage(byte[] data) {
		try {
			SchemaUpdateMessage schemaUpdateMessage = new SchemaUpdateMessage(data);
			this.schema = schemaUpdateMessage.getSchema();
			clusterHandler.handleSchemaUpdate(schema);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void handleInitialMessage(byte[] data) {
		try {
			handleInitialMessage(new InitMessage(data));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void handleInitialMessage(InitMessage initMessage) {
		lastTransactionId = initMessage.getLastTransactionId();
		currentTransactionId = initMessage.getCurrentTransactionId();
		transactionCount = initMessage.getTransactionCount();
		schema = initMessage.getSchema();
		clusterOperationMode = initMessage.getClusterOperationMode();
		clusterNodeRole = initMessage.getCurrentType();
		preferredClusterNodeRole = initMessage.getPreferredType();
		initialized = true;
		clusterHandler.handleNodeInitialized(this);
	}

	protected void handleInitialMessageResponse(byte[] data) {
		try {
			handleInitialMessageResponse(new InitMessageResponse(data));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void handleInitialMessageResponse(InitMessageResponse initMessage) {
		lastTransactionId = initMessage.getLastTransactionId();
		currentTransactionId = initMessage.getCurrentTransactionId();
		transactionCount = initMessage.getTransactionCount();
		schema = initMessage.getSchema();
		clusterOperationMode = initMessage.getClusterOperationMode();
		clusterNodeRole = initMessage.getCurrentType();
		preferredClusterNodeRole = initMessage.getPreferredType();
		initialized = true;
		clusterHandler.handleNodeInitializedResponse(this);
	}

	public boolean isInitialized() {
		return initialized;
	}

	public NetworkWriter getNetworkWriter() {
		return networkWriter;
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

	public ClusterNodeRole getClusterNodeRole() {
		return clusterNodeRole;
	}

	public ClusterNodeRole getPreferredClusterNodeRole() {
		return preferredClusterNodeRole;
	}

	public Schema getSchema() {
		return schema;
	}

	public int getHeadIdProposal() {
		return headIdProposal;
	}

	@Override
	public String toString() {
		return "id:" + nodeId + ", type:" + clusterNodeRole + ", last-tr:" + lastTransactionId;
	}
}
