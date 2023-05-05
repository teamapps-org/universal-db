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
