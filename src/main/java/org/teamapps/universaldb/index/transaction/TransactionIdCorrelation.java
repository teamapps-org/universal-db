/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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
package org.teamapps.universaldb.index.transaction;

import java.util.HashMap;
import java.util.Map;

public class TransactionIdCorrelation {

	private final Map<Integer, Integer> recordIdByCorrelationId;

	public TransactionIdCorrelation() {
		recordIdByCorrelationId = new HashMap<>();
	}

	public TransactionIdCorrelation(Map<Integer, Integer> recordIdByCorrelationId) {
		this.recordIdByCorrelationId = recordIdByCorrelationId;
	}

	public int getResolvedRecordIdByCorrelationId(int correlationId) {
		if (recordIdByCorrelationId.containsKey(correlationId)) {
			return recordIdByCorrelationId.get(correlationId);
		} else {
			return 0;
		}
	}

	public void putResolvedRecordIdForCorrelationId(int correlationId, int recordId) {
		recordIdByCorrelationId.put(correlationId, recordId);
	}

	public Map<Integer, Integer> getRecordIdByCorrelationId() {
		return recordIdByCorrelationId;
	}
}
