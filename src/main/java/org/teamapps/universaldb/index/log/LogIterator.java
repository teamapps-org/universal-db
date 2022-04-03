package org.teamapps.universaldb.index.log;

import java.io.*;
import java.util.Iterator;
import java.util.List;

public class LogIterator implements Iterator<byte[]>, AutoCloseable {

	private final List<File> logFiles;
	private int currentFileIndex = -1;
	private DataInputStream dis;
	private byte[] nextLog;

	public LogIterator(List<File> logFiles, long startPosition, boolean rotatingLogIndex) {
		this.logFiles = logFiles;
		seekLogPosition(startPosition, rotatingLogIndex);
		readLog();
	}

	private void seekLogPosition(long startPosition, boolean rotatingLogIndex) {
		try {
			long skipBytes = startPosition;
			if (rotatingLogIndex) {
				currentFileIndex = RotatingLogIndex.getFileIndex(startPosition);
				int filePos = RotatingLogIndex.getFilePos(startPosition);
				if (currentFileIndex >= logFiles.size()) {
					return;
				}
				skipBytes = filePos;
			} else {
				currentFileIndex = 0;
			}
			dis = new DataInputStream(new BufferedInputStream(new FileInputStream(logFiles.get(currentFileIndex)), 64_000));
			if (skipBytes > 0) {
				dis.skipNBytes(skipBytes);
			} else if (!rotatingLogIndex) {
				dis.readInt();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void readLog() {
		try {
			nextLog = null;
			if (dis == null) {
				currentFileIndex++;
				if (currentFileIndex >= logFiles.size()) {
					return;
				}
				dis = new DataInputStream(new BufferedInputStream(new FileInputStream(logFiles.get(currentFileIndex)), 64_000));
			}
			int size = dis.readInt();
			byte[] bytes = new byte[size];
			dis.readFully(bytes);
			nextLog = bytes;
		} catch (EOFException ignore) {
			closeStream();
			dis = null;
			readLog();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		if (nextLog != null) {
			return true;
		} else {
			closeStream();
			return false;
		}
	}

	@Override
	public byte[] next() {
		byte[] bytes = nextLog;
		readLog();
		return bytes;
	}

	@Override
	public void close() throws Exception {
		if (dis != null) {
			dis.close();
		}
	}

	private void closeStream() {
		try {
			if (dis == null) {
				return;
			}
			dis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
