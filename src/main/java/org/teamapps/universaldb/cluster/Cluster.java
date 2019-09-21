/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.cluster.message.*;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.cluster.network.ConnectionHandler;
import org.teamapps.universaldb.cluster.network.MessageType;
import org.teamapps.universaldb.cluster.network.NetworkWriter;
import org.teamapps.universaldb.cluster.network.NodeConnection;
import org.teamapps.universaldb.transaction.TransactionHandler;
import org.teamapps.universaldb.transaction.TransactionPacket;
import org.teamapps.universaldb.transaction.TransactionRequest;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Cluster extends Thread implements ConnectionHandler, ClusterHandler {

	private static final Logger log = LoggerFactory.getLogger(Cluster.class);

	private final ClusterConfig config;
	private final TransactionHandler transactionHandler;
	private ClusterOperationMode clusterOperationMode = ClusterOperationMode.BOOTING;
	private int localNodeId;
	private ClusterNodeRole localNodeType;
	private ClusterNodeRole preferredLocalNodeType;
	private int localPort;
	private ClusterNode headNode;
	private int clusterQuorum;
	private int electoralClusterNodes;

	private List<ClusterNode> clusterNodes;
	private Map<Integer, ClusterNode> nodeById;

	private LocalState localState = LocalState.CONNECTING;
	private boolean nonVotingNode;
	private int headElectionProposal;

	private TransactionRequest currentTransactionRequest;

	private Integer netWorkSyncObject = 10_001;

	enum ClusterState {
		MISSING_QUORUM,
		RUNNING,
		WAITING,
		ALL_NODES_READY_TO_SYNCHRONIZE,
	}

	enum LocalState {
		CONNECTING,
		SYNCHRONIZE_SCHEMA,
		SYNCHRONIZE_TRANSACTIONS,
		WAITING_FOR_PEER_SYNCHRONISATION,
		ELECTING_HEAD,
		WAIT_FOR_HEAD,
		RUNNING,
		RUNNING_AS_HEAD,
		ERROR,
	}

	public Cluster(ClusterConfig config, TransactionHandler transactionHandler) {
		this.config = config;
		this.transactionHandler = transactionHandler;
		clusterNodes = Collections.synchronizedList(new ArrayList<>());
		nodeById = new HashMap<>();
		ClusterNodeConfig localNode = config.getLocalNode();
		localNodeId = localNode.getNodeId();
		localPort = localNode.getPort();
		localNodeType = localNode.getPreferredNodeType().getBootUpType();
		preferredLocalNodeType = localNode.getPreferredNodeType();
		electoralClusterNodes++;
		for (ClusterNodeConfig node : config.getRemoteNodes()) {
			ClusterNode clusterNode = new ClusterNode(node, transactionHandler, this);
			clusterNodes.add(clusterNode);
			nodeById.put(clusterNode.getNodeId(), clusterNode);
			if (node.getPreferredNodeType().allowedToVoteForHeadSelection()) {
				electoralClusterNodes++;
			}
		}
		clusterQuorum = electoralClusterNodes / 2 + 1;
		if (!localNodeType.allowedToVoteForHeadSelection()) {
			nonVotingNode = true;
		}
		setName("cluster-socket");
		start();
		connectNodes();
		startNodeConnectionsCheck();
	}

	public synchronized void executeTransaction(TransactionRequest transactionRequest) {
		try {
			boolean isWaitingLogged = false;
			while (localState != LocalState.RUNNING && localState != LocalState.RUNNING_AS_HEAD) {
				if (!isWaitingLogged) {
					log.warn("Wait for transaction execution, node not available, " + getNodeStatusAsString());
				}
				isWaitingLogged = true;
				try {
					Thread.sleep(50);
				} catch (InterruptedException ignore) {
				}
			}
			if (localState == LocalState.RUNNING_AS_HEAD) {
				transactionHandler.executeTransactionRequest(transactionRequest);
				ResolvedTransactionRequest executionRequest = new ResolvedTransactionRequest(transactionRequest);
				for (ClusterNode clusterNode : clusterNodes) {
					if (clusterNode.isInitialized()) {
						clusterNode.sendMessage(executionRequest);
					}
				}
			} else if (localState == LocalState.RUNNING) {
				currentTransactionRequest = transactionRequest;
				UnresolvedTransactionRequest executionRequest = new UnresolvedTransactionRequest(transactionRequest);
				headNode.sendMessage(executionRequest);
				currentTransactionRequest.waitForExecution();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void connectNodes() {
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.getNodeId() < localNodeId) {
				connectNode(clusterNode);
			}
		}
	}

	private void connectNode(ClusterNode clusterNode) {
		try {
			clusterNode.setConnecting(true);
			NodeConnection connection = new NodeConnection(config.getClusterSecret(), clusterNode);
			InitMessage initMessage = new InitMessage(
					localNodeId,
					transactionHandler.getLastTransactionId(),
					transactionHandler.getCurrentTransactionId(),
					transactionHandler.getTransactionCount(),
					transactionHandler.getSchema(),
					clusterOperationMode, localNodeType,
					preferredLocalNodeType);
			connection.connect(clusterNode.getConfig().getAddress(), clusterNode.getConfig().getPort(), initMessage);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		while (true) {
			try {
				ServerSocket serverSocket = new ServerSocket(localPort);
				while (true) {
					Socket socket = serverSocket.accept();
					NodeConnection nodeConnection = new NodeConnection(config.getClusterSecret(), this);
					nodeConnection.setSocket(socket);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void handleMessage(MessageType messageType, byte[] data, NetworkWriter networkWriter) {
		if (messageType == MessageType.INITIAL_MESSAGE) {
			try {
				InitMessage initMessage = new InitMessage(data);
				ClusterNode clusterNode = nodeById.get(initMessage.getNodeId());
				networkWriter.setConnectionHandler(clusterNode);
				clusterNode.setNetworkWriter(networkWriter);
				clusterNode.handleInitialMessage(initMessage);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void handleConnectionError() {

	}


	@Override
	public ClusterOperationMode getClusterOperationMode() {
		return clusterOperationMode;
	}

	@Override
	public void handleNodeInitialized(ClusterNode node) {
		try {
			InitMessageResponse initMessageResponse = new InitMessageResponse(
					localNodeId,
					transactionHandler.getLastTransactionId(),
					transactionHandler.getCurrentTransactionId(),
					transactionHandler.getTransactionCount(),
					transactionHandler.getSchema(),
					clusterOperationMode,
					localNodeType,
					preferredLocalNodeType);
			node.sendMessage(initMessageResponse);
			log.info("Cluster node connected:" + node.getNodeId() + ", " + getNodeStatusAsString());
			checkClusterState();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void handleNodeInitializedResponse(ClusterNode clusterNode) {
		log.info("Cluster node connected (bd):" + clusterNode.getNodeId() + ", " + getNodeStatusAsString());
		checkClusterState();
	}

	@Override
	public void handleSchemaUpdate(Schema schema) {
		if (nonVotingNode) {
			try {
				transactionHandler.updateSchema(schema);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void handleSynchronizeTransactionRequest(final SynchronizeTransactionsRequest message, final NetworkWriter networkWriter) {
		if (message.getRequestEndId() <= transactionHandler.getLastTransactionId()) {
			log.info("Transaction synchronization request, req-from:" + message.getRequestStartId() + ", to:" + message.getRequestEndId() + ", " + getNodeStatusAsString());
			new Thread(() -> {
				Iterator<byte[]> transactions = transactionHandler.getTransactions(message.getRequestStartId(), message.getRequestEndId());
				while (transactions.hasNext()) {
					byte[] data = transactions.next();
					networkWriter.sendMessage(new SynchronizeTransactionResponse(data));
				}
				networkWriter.sendMessage(new SynchronizeTransactionsFinished(message.getRequestStartId(), message.getRequestEndId()));
			}).start();
		} else {
			log.error("Wrong transaction synchronization request, requested:" + message.getRequestEndId() + ", available:" + transactionHandler.getLastTransactionId() + ", " + getNodeStatusAsString());
		}
	}

	@Override
	public void handleSynchronizeTransactionResponse(SynchronizeTransactionResponse synchronizeTransactionResponse, ClusterNode clusterNode) {
		try {
			TransactionPacket packet = new TransactionPacket(synchronizeTransactionResponse.getData());
			//log.info("Handle synchronization transaction:" + packet.getTransactionId());
			transactionHandler.handleTransactionSynchronizationPacket(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void handleSynchronizeTransactionsFinished(SynchronizeTransactionsFinished synchronizeTransactionsFinished, ClusterNode node) {
		SynchronizeTransactionsStatus message = new SynchronizeTransactionsStatus(
				transactionHandler.getLastTransactionId(),
				transactionHandler.getCurrentTransactionId(),
				transactionHandler.getTransactionCount()
		);
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.isInitialized()) {
				clusterNode.sendMessage(message);
			}
		}
		localState = LocalState.WAITING_FOR_PEER_SYNCHRONISATION;
		log.info("Finished local transactions syncing:" + transactionHandler.getLastTransactionId() + " (" + synchronizeTransactionsFinished.getRequestEndId() + "), " + getNodeStatusAsString());
		checkSynchronizationState();
	}

	@Override
	public void handleSynchronizeTransactionsStatus(SynchronizeTransactionsStatus synchronizeTransactionsStatus, ClusterNode clusterNode) {
		if (localState == LocalState.WAITING_FOR_PEER_SYNCHRONISATION) {
			checkSynchronizationState();
		}
	}

	@Override
	public void handleHeadElectionProposal(int headIdProposal, ClusterNode clusterNode) {
		if (localState == LocalState.ELECTING_HEAD) {
			checkHeadElectionResult();
		}
	}

	@Override
	public void handleConnectToHeadRequest(ConnectToHeadRequest connectToHeadRequest, ClusterNode clusterNode) {
		if (localState == LocalState.RUNNING_AS_HEAD) {
			long requestedLastTransactionId = connectToHeadRequest.getLastTransactionId();
			long requestedTransactionCount = connectToHeadRequest.getTransactionCount();
			long lastTransactionId = transactionHandler.getLastTransactionId();
			long currentTransactionId = transactionHandler.getCurrentTransactionId();
			long transactionCount = transactionHandler.getTransactionCount();
			log.info("HEAD: Connected cluster node " + clusterNode.getNodeId() + " as " + clusterNode.getClusterNodeRole());

			//todo: send missing transactions!!!
			if (requestedLastTransactionId < lastTransactionId) {
				log.info("HEAD: send missing transactions, requested:" + requestedLastTransactionId + ", current:" + lastTransactionId + ", missing transactions:" + (transactionCount - requestedTransactionCount));
				new Thread(() -> {

				}).start();
			}
			clusterNode.sendMessage(new ConnectToHeadSuccessResponse(lastTransactionId, currentTransactionId, transactionCount));
		} else if (localState == LocalState.ELECTING_HEAD) {
			log.info("Cannot yet connect node to this HEAD:" + clusterNode.getNodeId() + ", " + getNodeStatusAsString());
			clusterNode.sendMessage(new ConnectToHeadWaitResponse(100));
		} else {
			log.warn("Error: node wants to connect to head, but this is no head!" + clusterNode.getNodeId() + ", this node:" + getNodeStatusAsString());
		}

	}

	@Override
	public void handleUnresolvedTransactionRequest(TransactionRequest transactionRequest) {
		executeTransaction(transactionRequest);
	}

	@Override
	public void handleResolvedTransactionRequest(TransactionRequest transactionRequest) {
		try {
			transactionHandler.executeTransactionRequest(transactionRequest);
			if (currentTransactionRequest != null && transactionRequest.getTransaction().getTransactionRequestId() == currentTransactionRequest.getTransaction().getTransactionRequestId()) {
				currentTransactionRequest.setExecuted();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void handleConnectToHeadWaitResponse(ConnectToHeadWaitResponse connectToHeadWaitResponse, ClusterNode clusterNode) {
		log.info("Must wait to connect to head:" + clusterNode.getNodeId() + ", " + getNodeStatusAsString());
	}

	@Override
	public void handleConnectToHeadSuccessResponse(ConnectToHeadSuccessResponse connectToHeadSuccessResponse, ClusterNode clusterNode) {
		log.info("Successfully connected to head:" + clusterNode.getNodeId() + ", " + getNodeStatusAsString());
		localState = LocalState.RUNNING;
	}

	@Override
	public void handleClusterNodeStatusUpdate(ClusterNodeStatusMessage clusterNodeStatusMessage, ClusterNode clusterNode) {

	}

	@Override
	public void handleLostConnection(ClusterNode node) {
		log.info("Lost connection to node:" + node);
		if (localState != LocalState.RUNNING) {
			if (getClusterState() == ClusterState.MISSING_QUORUM) {
				localState = LocalState.CONNECTING;
			}
		}
	}

	private boolean isAllClusterNodesSynced() {
		long lastId = transactionHandler.getLastTransactionId();
		long transactionCount = transactionHandler.getTransactionCount();
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.isInitialized()) {
				if (lastId != clusterNode.getLastTransactionId() || transactionCount != clusterNode.getTransactionCount()) {
					return false;
				}
			}
		}
		return true;
	}

	private boolean isLocalNodeSynced() {
		long lastId = transactionHandler.getLastTransactionId();
		long transactionCount = transactionHandler.getTransactionCount();
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.isInitialized()) {
				if (lastId < clusterNode.getLastTransactionId() || transactionCount < clusterNode.getTransactionCount()) {
					return false;
				}
			}
		}
		return true;
	}

	private void sendNodeStatus() {
		int countConnectedNodes = 0;
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.isInitialized()) {
				countConnectedNodes++;
			}
		}
		int currentHead = 0;
		if (headNode != null) {
			currentHead = headNode.getNodeId();
		}
		ClusterNodeStatusMessage nodeStatusMessage = new ClusterNodeStatusMessage(
				localNodeId,
				transactionHandler.getLastTransactionId(),
				transactionHandler.getCurrentTransactionId(),
				transactionHandler.getTransactionCount(),
				0, //todo
				clusterOperationMode,
				localNodeType,
				preferredLocalNodeType,
				countConnectedNodes,
				currentHead
		);
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.isInitialized()) {
				clusterNode.sendMessage(nodeStatusMessage);
			}
		}
	}

	private String getNodeStatusAsString() {
		int headId = 0;
		if (headNode != null) {
			headId = headNode.getNodeId();
		}
		return "nodeId:" + localNodeId + ", mode:" + clusterOperationMode + ", type:" + localNodeType + ", state:" + localState + ", transId:" + transactionHandler.getLastTransactionId() + ", trans-count:" + transactionHandler.getTransactionCount();
	}

	private ClusterState getClusterState() {
		log.info("Checking cluster state, " + getNodeStatusAsString());
		int countNodes = 1;
		int countRunningNodes = 0;
		boolean waitingNodes = false;
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.isInitialized() && clusterNode.getClusterNodeRole().allowedToVoteForHeadSelection()) {
				countNodes++;
				if (clusterNode.getClusterOperationMode() == ClusterOperationMode.RUNNING) {
					countRunningNodes++;
				} else if (clusterNode.getClusterOperationMode() == ClusterOperationMode.WAITING) {
					waitingNodes = true;
				}
			}
		}
		log.info("Cluster state: all nodes:" + countNodes + ", running:" + countRunningNodes + ", waiting:" + waitingNodes + ", quorum:" + clusterQuorum + ", electoral:" + electoralClusterNodes);
		if (countNodes >= clusterQuorum) {
			if (countRunningNodes >= clusterQuorum) {
				log.info("Cluster state: running");
				return ClusterState.RUNNING;
			} else if (waitingNodes && countNodes >= clusterQuorum) {
				log.info("Cluster state: waiting");
				return ClusterState.WAITING;
			} else if (countNodes >= electoralClusterNodes) {
				log.info("Cluster state: ready to sync");
				return ClusterState.ALL_NODES_READY_TO_SYNCHRONIZE;
			}
		}
		log.info("Cluster state: missing quorum");
		return ClusterState.MISSING_QUORUM;
	}

	private void checkClusterState() {
		synchronized (netWorkSyncObject) {
			log.info("pre check cluster state, " + getNodeStatusAsString());
			if (localState != LocalState.CONNECTING) {
				log.info("not checking cluster state as wrong local state:" + localState + ", " + getNodeStatusAsString());
				return;
			}
			if (clusterOperationMode != ClusterOperationMode.RUNNING) {
				if (getClusterState() != ClusterState.MISSING_QUORUM) {
					startSyncing();
				}
			}
		}
	}

	private void checkSynchronizationState() {
		synchronized (netWorkSyncObject) {
			if (localState != LocalState.WAITING_FOR_PEER_SYNCHRONISATION) {
				return;
			}
			if (!isLocalNodeSynced()) {
				log.info("Synchronize transactions again..., " + getNodeStatusAsString());
				synchronizeTransactions();
				return;
			}
			if (getClusterState() == ClusterState.RUNNING) {
				int headId = 0;
				for (ClusterNode clusterNode : clusterNodes) {
					if (clusterNode.isInitialized()) {
						if (clusterNode.getClusterNodeRole() == ClusterNodeRole.HEAD) {
							headId = clusterNode.getNodeId();
							break;
						}
					}
				}
				connectToHead(headId);
			} else {
				if (isAllClusterNodesSynced()) {
					startHeadElection();
				}
			}
		}
	}

	private void startHeadElection() {
		localState = LocalState.ELECTING_HEAD;
		headElectionProposal = selectBestHead();
		log.info("Start electing head, proposal is:" + headElectionProposal + ", " + getNodeStatusAsString());
		HeadElectionProposal headElectionProposalMessage = new HeadElectionProposal(headElectionProposal);
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.isInitialized()) {
				clusterNode.sendMessage(headElectionProposalMessage);
			}
		}
	}

	private void checkHeadElectionResult() {
		int countResults = 0;
		if (headElectionProposal > 0) {
			for (ClusterNode clusterNode : clusterNodes) {
				if (clusterNode.isInitialized()) {
					if (headElectionProposal != clusterNode.getHeadIdProposal()) {
						return;
					} else {
						countResults++;
					}
				}
			}
			if (countResults < clusterQuorum) {
				log.error("Error: head election with all results but less than quorum:" + countResults + ", quorum:" + clusterQuorum);
			}
			if (localNodeId == headElectionProposal) {
				localState = LocalState.RUNNING_AS_HEAD;
				clusterOperationMode = ClusterOperationMode.RUNNING;
				sendNodeStatus();
				log.info("Start as HEAD node, " + getNodeStatusAsString());
			} else {
				log.info("Connect to HEAD:" + headElectionProposal + ", " + getNodeStatusAsString());
				connectToHead(headElectionProposal);
			}
		}
	}

	private void connectToHead(int headId) {
		localState = LocalState.WAIT_FOR_HEAD;
		ClusterNode clusterNode = nodeById.get(headId);
		headNode = clusterNode;
		headNode.sendMessage(
				new ConnectToHeadRequest(
						transactionHandler.getLastTransactionId(),
						transactionHandler.getCurrentTransactionId(),
						transactionHandler.getTransactionCount()))
		;
	}

	private int selectBestHead() {
		List<ClusterNodeConfig> connectedNodes = new ArrayList<>();
		for (ClusterNodeConfig clusterNode : config.getClusterNodes()) {
			int nodeId = clusterNode.getNodeId();
			if (localNodeId == nodeId) {
				connectedNodes.add(clusterNode);
			} else {
				ClusterNode node = nodeById.get(nodeId);
				if (node.isInitialized()) {
					connectedNodes.add(clusterNode);
				}
			}
		}
		ClusterNodeConfig head = connectedNodes.stream().filter(node -> node.getPreferredNodeType() == ClusterNodeRole.HEAD).findAny().orElse(null);
		if (head == null) {
			head = connectedNodes.stream().filter(node -> node.getPreferredNodeType() == ClusterNodeRole.AUTO).findAny().orElse(null);
		}
		if (head == null) {
			head = connectedNodes.stream().filter(node -> node.getPreferredNodeType() == ClusterNodeRole.WORKER).findAny().orElse(null);
		}
		if (head == null) {
			head = connectedNodes.stream().filter(node -> node.getPreferredNodeType() == ClusterNodeRole.SNAPSHOT_PROVIDER).findAny().orElse(null);
		}
		if (head == null) {
			log.warn("Could not find any node that can act as head of cluster! " + getNodeStatusAsString());
			return 0;
		} else {
			return head.getNodeId();
		}
	}

	private void startSyncing() {
		try {
			synchronizeSchema();
			synchronizeTransactions();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void synchronizeSchema() throws IOException {
		log.info("Start synchronizing schema, " + getNodeStatusAsString());
		localState = LocalState.SYNCHRONIZE_SCHEMA;
		Schema localSchema = transactionHandler.getSchema();
		boolean modifiedSchema = false;
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.getClusterNodeRole().allowedToVoteForHeadSelection() && clusterNode.isInitialized()) {
				Schema schema = clusterNode.getSchema();
				if (!localSchema.isSameSchema(schema)) {
					if (localSchema.isCompatibleWith(schema)) {
						localSchema.merge(schema);
						modifiedSchema = true;
					} else {
						localState = LocalState.ERROR;
						log.error("Cluster node boot: incompatible schemas of node:" + clusterNode.getNodeId() + ", with schema:" + schema + ", local-schema:" + localSchema + ", " + getNodeStatusAsString());
						//todo: shutdown node?
						return;
					}
				}
			}
		}
		if (modifiedSchema) {
			log.info("Cluster needs to update schema - new compatible schema:" + localSchema + ", " + getNodeStatusAsString());
			transactionHandler.updateSchema(localSchema);
			if (!nonVotingNode) {
				for (ClusterNode clusterNode : clusterNodes) {
					if (clusterNode.isInitialized()) {
						clusterNode.sendSchemaUpdate(localSchema);
					}
				}
			}
		}
	}

	private void synchronizeTransactions() {
		log.info("Start synchronizing transactions, " + getNodeStatusAsString());
		localState = LocalState.SYNCHRONIZE_TRANSACTIONS;
		long localTransactionId = transactionHandler.getLastTransactionId();
		long highestTransactId = localTransactionId;
		List<ClusterNode> currentClusterNodes = new ArrayList<>();
		for (ClusterNode clusterNode : clusterNodes) {
			if (clusterNode.isInitialized()) {
				if (clusterNode.getLastTransactionId() > localTransactionId) {
					if (clusterNode.getLastTransactionId() > highestTransactId) {
						currentClusterNodes.clear();
						currentClusterNodes.add(clusterNode);
						highestTransactId = clusterNode.getLastTransactionId();
					} else if (clusterNode.getLastTransactionId() == highestTransactId) {
						currentClusterNodes.add(clusterNode);
					}
				}
			}
		}
		if (!currentClusterNodes.isEmpty()) {
			ClusterNode bestSynchronizationNode = getBestSynchronizationNode(currentClusterNodes);
			log.info("Cluster node needs to synchronize " + (bestSynchronizationNode.getTransactionCount() - transactionHandler.getTransactionCount()) + ", last local transaction:" + transactionHandler.getLastTransactionId() + ", current transaction:" + bestSynchronizationNode.getLastTransactionId());
			bestSynchronizationNode.sendMessage(
					new SynchronizeTransactionsRequest(
							transactionHandler.getLastTransactionId(),
							bestSynchronizationNode.getLastTransactionId(),
							transactionHandler.getLastTransactionId(),
							transactionHandler.getCurrentTransactionId(),
							transactionHandler.getTransactionCount()
					)
			);
		} else {
			localState = LocalState.WAITING_FOR_PEER_SYNCHRONISATION;
			checkSynchronizationState();
		}
	}

	private ClusterNode getBestSynchronizationNode(List<ClusterNode> clusterNodes) {
		ClusterNode clusterNode = clusterNodes.stream().filter(node -> node.getClusterNodeRole() == ClusterNodeRole.SNAPSHOT_PROVIDER).findAny().orElse(null);
		if (clusterNode == null) {
			clusterNode = clusterNodes.stream().filter(node -> node.getClusterNodeRole() == ClusterNodeRole.TRANSACTION_LOGGER).findAny().orElse(null);
		}
		if (clusterNode == null) {
			clusterNode = clusterNodes.stream().filter(node -> node.getClusterNodeRole() == ClusterNodeRole.WORKER).findAny().orElse(null);
		}
		if (clusterNode == null) {
			clusterNode = clusterNodes.stream().filter(node -> node.getClusterNodeRole() == ClusterNodeRole.AUTO).findAny().orElse(null);
		}
		if (clusterNode == null) {
			clusterNode = clusterNodes.stream().filter(node -> node.getClusterNodeRole() == ClusterNodeRole.HEAD).findAny().orElse(null);
		}
		if (clusterNode != null) {
			return clusterNode;
		}
		return clusterNodes.get(0);
	}

	private void startNodeConnectionsCheck() {
		Thread thread = new Thread(() -> {
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			while (true) {
				try {
					Thread.sleep(1000);
					for (ClusterNode clusterNode : clusterNodes) {
						if (!clusterNode.isConnected() && !clusterNode.isConnecting() && clusterNode.getNodeId() < localNodeId) {
							try {
								connectNode(clusterNode);
							} catch (Exception ignore) {
							}
						}
					}
				} catch (Exception ignore) {
				}
			}
		}, "cluster-connect-loop");
		thread.setDaemon(true);
		thread.start();
	}

	private void startClusterNodeUpdater() {
		Thread thread = new Thread(() -> {
			while (true) {
				try {
					Thread.sleep(1000);
					//todo: send cluster state info to every node
					//send every 1 sec in non running mode, else every 5 secs

				} catch (Exception ignore) {
				}
			}
		}, "cluster-node-updater");
		thread.setDaemon(true);
		thread.start();
	}

}
