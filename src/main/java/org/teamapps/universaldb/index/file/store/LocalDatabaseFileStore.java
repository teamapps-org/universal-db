package org.teamapps.universaldb.index.file.store;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class LocalDatabaseFileStore implements DatabaseFileStore {

	private final File basePath;

	public LocalDatabaseFileStore(File basePath) {
		this.basePath = basePath;
	}

	@Override
	public boolean isEncrypted() {
		return false;
	}

	@Override
	public File getLocalFile(String hash, long length, String key) {
		return FileStoreUtil.getPath(basePath, hash, length);
	}

	@Override
	public File loadRemoteFile(String hash, long length, String key) {
		return null;
	}

	@Override
	public String storeFile(File file, String hash, long length)  {
		try {
			File storeFile = FileStoreUtil.getPath(basePath, hash, length, true);
			if (!storeFile.exists() || storeFile.length() != length) {
				Files.copy(file.toPath(), storeFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
			return null;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
