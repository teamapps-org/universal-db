package org.teamapps.universaldb.index.fileng;

import java.io.File;
import java.io.InputStream;

public interface RemoteFileStore {

	InputStream getInputStream(String path) throws Exception;

	File getFile(String path) throws Exception;

	void setInputStream(String path, InputStream inputStream, long length) throws Exception;

	void setFile(String path, File file) throws Exception;

	boolean fileExists(String path);

	void removeFile(String path) throws Exception;

}
