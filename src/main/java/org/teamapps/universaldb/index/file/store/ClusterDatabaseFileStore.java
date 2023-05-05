package org.teamapps.universaldb.index.file.store;

import java.io.File;

public class ClusterDatabaseFileStore implements DatabaseFileStore{
	@Override
	public boolean isEncrypted() {
		return true;
	}

	@Override
	public File getLocalFile(String hash, long length, String key) {
		return null;
	}

	@Override
	public File loadRemoteFile(String hash, long length, String key) {
		return null;
	}

	@Override
	public String storeFile(File file, String hash, long length) {
		return null;
	}
}
