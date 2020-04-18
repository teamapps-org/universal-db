package org.teamapps.universaldb.index.fileng;

import org.teamapps.universaldb.index.file.FileUtil;

import java.io.*;

public class LocalFileValue implements FileValue{

	private final File file;
	private final String name;
	private final String hash;

	public LocalFileValue(File file) {
		this(file, file.getName());
	}

	public LocalFileValue(File file, String fileName) {
		this.file = file;
		this.name = fileName;
		this.hash = FileUtil.createFileHash(file);
	}

	@Override
	public String getHash() {
		return hash;
	}

	@Override
	public String getFileName() {
		return name;
	}

	@Override
	public long getSize() {
		return file.length();
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return new FileInputStream(file);
	}

	@Override
	public File getAsFile() {
		return file;
	}

	@Override
	public int getVersion() {
		return 0;
	}
}
