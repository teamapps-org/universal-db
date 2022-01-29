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

import java.util.HashMap;
import java.util.Map;

public enum ClusterNodeState {

	CONNECTING(1),
	SYNCHRONIZE_SCHEMA(2),
	SYNCHRONIZE_TRANSACTIONS(3),
	WAITING_FOR_PEER_SYNCHRONISATION(4),
	ELECTING_HEAD(5),
	WAIT_FOR_HEAD(6),
	RUNNING(7),
	RUNNING_AS_HEAD(8),
	ERROR(9),
	;

	private static Map<Integer, ClusterNodeState> valueById = new HashMap<>();

	static {
		for (ClusterNodeState value : values()) {
			valueById.put(value.getId(), value);
		}
	}

	public static ClusterNodeState getById(int id) {
		return valueById.get(id);
	}

	private final int id;

	ClusterNodeState(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

}
