package org.teamapps.universaldb.index.log;

public interface LogIndex {

	long writeLog(byte[] bytes);

	long writeLogCommitted(byte[] bytes);

	byte[] readLog(long pos);

	long getPosition();

	void flush();

	void close();

	void drop();
}
