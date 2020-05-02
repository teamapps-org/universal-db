package org.teamapps.universaldb.index.fileng;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileTextContentIndex {

	private File indexFile;
	private BufferedOutputStream outputStream;

	public FileTextContentIndex(File path, String name) {
		indexFile = new File(path, name + ".idt");
	}

	public void addEntry(int recordId, int version, int length, String contentText) throws IOException {
		addEntry(new FileFullTextContentEntry(recordId, version, length, contentText));
	}

	public void addEntry(FileFullTextContentEntry entry) throws IOException {
		if (outputStream == null) {
			outputStream = new BufferedOutputStream(new FileOutputStream(indexFile), 32_000);
		}
		outputStream.write(entry.getIndexValue());
		outputStream.flush();
	}

	public FileFullTextContentEntryIterator getEntryIterator() throws IOException {
		return new FileFullTextContentEntryIterator(indexFile);
	}

	public void close() {
		if (outputStream != null) {
			try {
				outputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}


	public void drop() {
		indexFile.delete();
	}
}
