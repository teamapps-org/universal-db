package org.teamapps.universaldb.analysis;

import org.teamapps.universaldb.index.log.DefaultLogIndex;
import org.teamapps.universaldb.index.log.LogIterator;
import org.teamapps.universaldb.index.log.RotatingLogIndex;
import org.teamapps.universaldb.index.transaction.resolved.ResolvedTransaction;
import org.teamapps.universaldb.index.transaction.schema.ModelUpdate;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TransactionLogAnalyzer {


	public static void analyzeLog(File path) {
		RotatingLogIndex transactionLog = new RotatingLogIndex(path, "transactions");
		DefaultLogIndex modelsLog = new DefaultLogIndex(path, "models");

		List<ModelUpdate> modelUpdates = getModelUpdates(modelsLog);

		LogIterator logIterator = transactionLog.readLogs();
		while (logIterator.hasNext()) {
			ResolvedTransaction transaction = ResolvedTransaction.createResolvedTransaction(logIterator.next());

		}
		logIterator.closeSave();
	}

	public synchronized static List<ModelUpdate> getModelUpdates(DefaultLogIndex modelsLog) {
		if (modelsLog.isEmpty()) {
			return Collections.emptyList();
		}
		return modelsLog.readAllLogs()
				.stream()
				.map(ModelUpdate::new)
				.collect(Collectors.toList());
	}
}
