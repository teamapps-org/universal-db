package org.teamapps.universaldb.index.fileng;

import java.io.File;
import java.io.InputStream;

public interface FileStore {

	String getHash(int id);

	String getFileName(int id);

	long getSize(int id);

	InputStream getInputStream(int id);

	File getAsFile(int id);

	File getFileVersion(int id, int version);

	int getVersion(int id);
}
