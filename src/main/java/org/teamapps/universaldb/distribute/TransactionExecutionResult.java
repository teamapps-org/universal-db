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
package org.teamapps.universaldb.distribute;

import java.util.Map;

public class TransactionExecutionResult {

	private volatile boolean success;
	private volatile boolean executed;
	private volatile Map<Integer, Integer> recordIdByCorrelationId;

	public boolean isSuccess() {
		return success;
	}

	public boolean isExecuted() {
		return executed;
	}

	public Map<Integer, Integer> getRecordIdByCorrelationId() {
		return recordIdByCorrelationId;
	}

	public synchronized void waitForExecution() {
		while (!executed) {
			try {
				wait();
			} catch (InterruptedException ignore) { }
		}
	}

	public void handleError(){
		success = false;
		executed = true;
		notifyAll();
	}

	public void handleSuccess(Map<Integer, Integer> recordIdByCorrelationId) {
		this.recordIdByCorrelationId = recordIdByCorrelationId;
		success = true;
		executed = true;
		notifyAll();
	}
}
