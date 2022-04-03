package org.teamapps.universaldb.index.log;

import java.util.ArrayList;
import java.util.List;

public interface LogIndex {

	default long writeLog(byte[] bytes) {
		return writeLog(bytes, true);
	}

	long writeLog(byte[] bytes, boolean committed);

	byte[] readLog(long pos);

	LogIterator readLogs();

	LogIterator readLogs(long pos);

	default List<byte[]> readAllLogs() {
		LogIterator logIterator = readLogs();
		List<byte[]> logs = new ArrayList<>();
		while (logIterator.hasNext()) {
			logs.add(logIterator.next());
		}
		try {
			logIterator.close();
		} catch (Exception e) {
			throw new RuntimeException("Error closing log iterator", e);
		}
		return logs;
	}

	long getPosition();

	boolean isEmpty();

	void flush();

	void close();

	void drop();
}
