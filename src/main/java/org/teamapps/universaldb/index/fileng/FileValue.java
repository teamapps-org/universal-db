package org.teamapps.universaldb.index.fileng;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public interface FileValue {

	static FileValue create(File file) {
		return new LocalFileValue(file);
	}

	static FileValue create(File file, String fileName) {
		return new LocalFileValue(file, fileName);
	}

	String getHash();

	String getFileName();

	long getSize();

	InputStream getInputStream() throws IOException;

	File getAsFile();

	int getVersion();
}
