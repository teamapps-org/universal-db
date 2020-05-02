package org.teamapps.universaldb.index.fileng;

import java.io.*;
import java.util.Iterator;

public class FileFullTextContentEntryIterator implements Iterator<FileFullTextContentEntry> {
	private final DataInputStream dataInputStream;
	private FileFullTextContentEntry nextEntry;

	public FileFullTextContentEntryIterator(File indexFile) throws IOException {
		DataInputStream dataInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile), 128_000));
		this.dataInputStream = dataInputStream;
		fetchNext();
	}

	@Override
	public boolean hasNext() {
		return nextEntry != null;
	}

	@Override
	public FileFullTextContentEntry next() {
		FileFullTextContentEntry next = nextEntry;
		fetchNext();
		return next;
	}

	private void fetchNext() {
		try {
			nextEntry = new FileFullTextContentEntry(dataInputStream);
		} catch (EOFException e) {
			nextEntry = null;
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			dataInputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
