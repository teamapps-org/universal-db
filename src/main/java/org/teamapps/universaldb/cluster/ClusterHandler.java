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

import org.teamapps.universaldb.cluster.message.*;
import org.teamapps.universaldb.schema.Schema;
import org.teamapps.universaldb.cluster.network.NetworkWriter;
import org.teamapps.universaldb.transaction.TransactionRequest;

public interface ClusterHandler {

	ClusterOperationMode getClusterOperationMode();

	void handleNodeInitialized(ClusterNode node);

	void handleNodeInitializedResponse(ClusterNode clusterNode);

	void handleSchemaUpdate(Schema schema);

	void handleSynchronizeTransactionRequest(SynchronizeTransactionsRequest message, NetworkWriter networkWriter);

	void handleSynchronizeTransactionResponse(SynchronizeTransactionResponse synchronizeTransactionResponse, ClusterNode clusterNode);

	void handleSynchronizeTransactionsFinished(SynchronizeTransactionsFinished synchronizeTransactionsFinished, ClusterNode clusterNode);

	void handleSynchronizeTransactionsStatus(SynchronizeTransactionsStatus synchronizeTransactionsStatus, ClusterNode clusterNode);

	void handleHeadElectionProposal(int headIdProposal, ClusterNode clusterNode);

	void handleClusterNodeStatusUpdate(ClusterNodeStatusMessage clusterNodeStatusMessage, ClusterNode clusterNode);

	void handleConnectToHeadSuccessResponse(ConnectToHeadSuccessResponse connectToHeadSuccessResponse, ClusterNode clusterNode);

	void handleConnectToHeadWaitResponse(ConnectToHeadWaitResponse connectToHeadWaitResponse, ClusterNode clusterNode);

	void handleConnectToHeadRequest(ConnectToHeadRequest connectToHeadRequest, ClusterNode clusterNode);

	void handleUnresolvedTransactionRequest(TransactionRequest transactionRequest);

	void handleResolvedTransactionRequest(TransactionRequest transactionRequest);

	void handleLostConnection(ClusterNode node);
}
