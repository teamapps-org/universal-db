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

	public void waitForExecution() {
		while (executed) {
			try {
				wait();
			} catch (InterruptedException ignore) { }
		}
	}

	public void handleError(){
		success = false;
		executed = true;
	}

	public void handleSuccess(Map<Integer, Integer> recordIdByCorrelationId) {
		this.recordIdByCorrelationId = recordIdByCorrelationId;
		success = true;
		executed = true;
		notifyAll();
	}
}
