package org.teamapps.universaldb.index.file2;

import java.io.File;
import java.io.InputStream;

public class LocalFileStore implements FileStore{


	@Override
	public boolean isEncrypted() {
		return false;
	}

//	@Override
//	public String addFile(File file, long length, String hash) {
//		return null;
//	}
//
//	@Override
//	public InputStream getFile(String hash, long length, String password) {
//		return null;
//	}
}
