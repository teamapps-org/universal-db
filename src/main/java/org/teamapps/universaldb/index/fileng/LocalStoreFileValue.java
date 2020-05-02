package org.teamapps.universaldb.index.fileng;

import java.io.*;

public class LocalStoreFileValue implements FileValue {

	private final int id;
	private final FileStore fileStore;
	private final File file;

	public LocalStoreFileValue(int id, FileStore fileStore, File file) {
		this.id = id;
		this.fileStore = fileStore;
		this.file = file;
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
		try {
			return new FileInputStream(file);
		} catch (FileNotFoundException ignored) { /* this is checked before */}
		return null;
	}

	@Override
	public File getAsFile() {
		return file;
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
