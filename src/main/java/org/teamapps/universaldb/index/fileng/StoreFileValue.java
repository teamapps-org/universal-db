package org.teamapps.universaldb.index.fileng;

import java.io.File;
import java.io.InputStream;

public class StoreFileValue implements FileValue {

	private final int id;
	private final FileStore fileStore;

	public StoreFileValue(int id, FileStore fileStore) {
		this.id = id;
		this.fileStore = fileStore;
	}

	@Override
	public String getHash() {
		return fileStore.getHash(id);
	}

	@Override
	public String getFileName() {
		return fileStore.getFileName(id);
	}

	@Override
	public long getSize() {
		return fileStore.getSize(id);
	}

	@Override
	public InputStream getInputStream() {
		return fileStore.getInputStream(id);
	}

	@Override
	public File getAsFile() {
		return fileStore.getAsFile(id);
	}

	@Override
	public int getVersion() {
		return fileStore.getVersion(id);
	}

	@Override
	public File getFileVersion(int version) {
		return fileStore.getFileVersion(id, version);
	}
}
