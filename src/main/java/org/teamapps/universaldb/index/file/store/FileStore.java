package org.teamapps.universaldb.index.file.store;

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileValue;

import java.io.IOException;
import java.util.function.Supplier;

public interface FileStore {

	boolean isEncrypted();

	FileValue getFile(String name, String hash, long length, String key, Supplier<FileContentData> contentDataSupplier);

	FileValue storeFile(FileValue fileValue) throws IOException;

}
