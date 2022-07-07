package org.teamapps.universaldb.index.log;

import org.teamapps.protocol.file.FileProvider;
import org.teamapps.protocol.file.FileSink;
import org.teamapps.protocol.schema.MessageObject;
import org.teamapps.protocol.schema.ModelRegistry;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class MessageLog {

	private LogIndex logIndex;
	private FileProvider fileProvider;
	private FileSink fileSink;
	private ModelRegistry modelRegistry;

	public MessageLog(File basePath, String name, boolean rotating, FileProvider fileProvider, FileSink fileSink, ModelRegistry modelRegistry) {
		this.fileProvider = fileProvider;
		this.fileSink = fileSink;
		this.modelRegistry = modelRegistry;
		this.logIndex = rotating ? new RotatingLogIndex(basePath, name) : new DefaultLogIndex(basePath, name);

	}

	public long addMessage(MessageObject message) throws IOException {
		byte[] bytes = message.toBytes(fileSink);
		return logIndex.writeLog(bytes);
	}

	public MessageObject readMessage(long position) throws IOException {
		byte[] bytes = logIndex.readLog(position);
		return new MessageObject(bytes, modelRegistry, fileProvider, null);
	}

	public Iterator<MessageObject> getMessages(long startPos) {
		LogIterator logIterator = logIndex.readLogs(startPos);
		return new Iterator<>() {
			@Override
			public boolean hasNext() {
				return logIterator.hasNext();
			}

			@Override
			public MessageObject next() {
				byte[] bytes = logIterator.next();
				try {
					return new MessageObject(bytes, modelRegistry, fileProvider, null);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}


	public void close() {
		logIndex.close();
	}
}
