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
package org.teamapps.universaldb.cluster;

import java.util.List;
import java.util.stream.Collectors;

public class ClusterConfig {

	private final String clusterSecret;
	private final List<ClusterNodeConfig> clusterNodes;
	private final int localNodeId;


	public ClusterConfig(String clusterSecret, List<ClusterNodeConfig> clusterNodes, int localNodeId) {
		this.clusterSecret = clusterSecret;
		this.clusterNodes = clusterNodes;
		this.localNodeId = localNodeId;
	}

	public ClusterConfig copy(int localNodeId) {
		return new ClusterConfig(clusterSecret,clusterNodes, localNodeId);
	}

	public List<ClusterNodeConfig> getClusterNodes() {
		return clusterNodes;
	}

	public List<ClusterNodeConfig> getRemoteNodes() {
		return clusterNodes.stream().filter(node -> node.getNodeId() != localNodeId).collect(Collectors.toList());
	}

	public ClusterNodeConfig getLocalNode() {
		return clusterNodes.stream().filter(node -> node.getNodeId() == localNodeId).findFirst().orElse(null);
	}

	public String getClusterSecret() {
		return clusterSecret;
	}

	public int getLocalNodeId() {
		return localNodeId;
	}
}
