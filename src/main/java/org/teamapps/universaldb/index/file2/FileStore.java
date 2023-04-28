package org.teamapps.universaldb.index.file2;

import org.teamapps.message.protocol.file.FileData;

import java.io.File;
import java.io.InputStream;

public interface FileStore {

	boolean isEncrypted();
//
//	String addFile(File file, long length, String hash);
//
//	FileData addFile(FileData fileData);
//
//	InputStream getFile(String hash, long length, String password);
}
