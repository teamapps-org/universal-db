package org.teamapps.universaldb.index.file.store;

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.file.value.CommittedLocalFile;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

public class LocalFileStore implements FileStore{

	private final File basePath;
//	private final Function<Integer, FileContentData> contentDataProvider;

	public LocalFileStore(File path, String name) {
		basePath = new File(path, name);
		basePath.mkdir();
	}

	@Override
	public boolean isEncrypted() {
		return false;
	}

	@Override
	public FileValue getFile(String name, String hash, long length, String key, Supplier<FileContentData> contentDataSupplier) {
		File storeFile = getStoreFile(hash, length);
		return new CommittedLocalFile(storeFile, name, hash, length, contentDataSupplier);
	}

	@Override
	public FileValue storeFile(FileValue fileValue) throws IOException {
		if (fileValue == null || fileValue.getSize() == 0) {
			return null;
		}
		File filePath = createPath(fileValue.getHash(), fileValue.getSize());
		if (!filePath.exists() || filePath.length() != fileValue.getSize()) {
			fileValue.copyToFile(filePath);
		}
		return new CommittedLocalFile(filePath, fileValue.getFileName(), fileValue.getHash(), fileValue.getSize(), fileValue::getFileContentData);
	}

	private File getStoreFile(String hash, long length) {
		return FileStoreUtil.getPath(basePath, hash, length);
	}

	private File createPath(String hash, long length) {
		File filePath = getStoreFile(hash, length);
		if (!filePath.getParentFile().exists()) {
			filePath.getParentFile().mkdirs();
		}
		return filePath;
	}


}
