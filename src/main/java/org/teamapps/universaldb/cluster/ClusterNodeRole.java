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
package org.teamapps.universaldb.cluster;

public enum ClusterNodeRole {

	STAND_ALONE(0), //no cluster
	AUTO(1), //any role: head, worker, snapshot-provider
	HEAD(2),
	WORKER(3),
	SNAPSHOT_PROVIDER(4), //creates snapshots, may not become head
	TRANSACTION_LOGGER(5), //NO DB only transaction log - but with vote rights, nodes should sync with this node if node snapshot provider is there
	REMOTE(6), //counts as member, may never be head
	EXTERNAL(7), //may read and write but has no voting right and is never head
	EXTERNAL_SUBSET(8), //external application that has only access to a single db (filter transactions for db), schema is only merged for registered db
	SNAPSHOT_CONSUMER(9), //only sync - if empty, never write, never vote -> sync to specific date/transaction an then run as independent node


	;
	public static ClusterNodeRole getById(int id) {
		for (ClusterNodeRole value : values()) {
			if (value.getId() == id) {
				return value;
			}
		}
		return null;
	}

	private final int id;

	ClusterNodeRole(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public ClusterNodeRole getBootUpType() {
		switch (this) {
			case STAND_ALONE: return STAND_ALONE;
			case AUTO:
			case HEAD:
			case WORKER:
				return AUTO;
			case SNAPSHOT_PROVIDER: return SNAPSHOT_PROVIDER;
			case REMOTE: return REMOTE;
			case TRANSACTION_LOGGER: return TRANSACTION_LOGGER;
			case EXTERNAL: return EXTERNAL;
			case EXTERNAL_SUBSET: return EXTERNAL_SUBSET;
			case SNAPSHOT_CONSUMER: return SNAPSHOT_CONSUMER;
		}
		return null;
	}

	public boolean allowedToVoteForHeadSelection() {
		switch (this) {
			case AUTO:
			case HEAD:
			case WORKER:
			case SNAPSHOT_PROVIDER:
			case TRANSACTION_LOGGER:
			case REMOTE:
				return true;
			case EXTERNAL:
			case EXTERNAL_SUBSET:
			case SNAPSHOT_CONSUMER:
			case STAND_ALONE:
				return false;
		}
		return false;
	}

	public boolean allowedToBecomeHead() {
		switch (this) {
			case AUTO:
			case HEAD:
			case WORKER:
				return true;
			case SNAPSHOT_PROVIDER:
			case REMOTE:
			case TRANSACTION_LOGGER:
			case EXTERNAL:
			case EXTERNAL_SUBSET:
			case SNAPSHOT_CONSUMER:
			case STAND_ALONE:
				return false;
		}
		return false;
	}

}
