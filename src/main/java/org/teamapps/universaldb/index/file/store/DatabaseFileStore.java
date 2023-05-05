package org.teamapps.universaldb.index.file.store;

import java.io.File;

public interface DatabaseFileStore {

	boolean isEncrypted();

	File getLocalFile(String hash, long length, String key);

	File loadRemoteFile(String hash, long length, String key);

	String storeFile(File file, String hash, long length);
}
