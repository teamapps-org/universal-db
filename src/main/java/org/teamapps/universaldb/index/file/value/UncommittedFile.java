package org.teamapps.universaldb.index.file.value;

import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class UncommittedFile implements FileValue {

	private final File file;
	private final String fileName;
	private final String hash;
	private final long size;
	private final FileContentParser contentParser;
	private FileContentData contentData;

	public UncommittedFile(File file) {
		this(file, file.getName());
	}

	public UncommittedFile(File file, String fileName) {
		this.file = file;
		this.fileName = fileName;
		this.size = file.length();
		this.contentParser = new FileContentParser(file, fileName);
		this.hash = contentParser.getHash();
	}

	@Override
	public FileValueType getType() {
		return FileValueType.UNCOMMITTED_FILE;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return new BufferedInputStream(new FileInputStream(file));
	}

	@Override
	public File getAsFile() {
		return file;
	}

	@Override
	public void copyToFile(File file) throws IOException {
		Files.copy(this.file.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}

	@Override
	public String getFileName() {
		return fileName;
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public String getHash() {
		return hash;
	}

	@Override
	public String getKey() {
		return null;
	}

	@Override
	public FileContentData getFileContentData() {
		return getFileContentData(100_000);
	}

	@Override
	public FileContentData getFileContentData(int maxContentLength) {
		if (contentData == null) {
			contentData = contentParser.getFileContentData(maxContentLength);
		}
		return contentData;
	}

	@Override
	public String getDetectedLanguage() {
		if (getFileContentData() != null) {
			if (contentData.getLanguage() == null && contentParser != null) {
				contentParser.getContentLanguage();
			}
			return contentData.getLanguage();
		} else {
			return null;
		}
	}






}
