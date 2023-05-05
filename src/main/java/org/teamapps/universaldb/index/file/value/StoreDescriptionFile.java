package org.teamapps.universaldb.index.file.value;

import org.teamapps.message.protocol.utils.MessageUtils;
import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileValue;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class StoreDescriptionFile implements FileValue {

	private final File file;
	private final String fileName;
	private final long size;
	private final String hash;
	private final String key;
	private final FileContentData contentData;

	public StoreDescriptionFile(File file, String fileName, long size, String hash, String key, FileContentData contentData) {
		this.file = file;
		this.fileName = fileName;
		this.size = size;
		this.hash = hash;
		this.key = key;
		this.contentData = contentData;
	}

	public StoreDescriptionFile(DataInputStream dis) throws IOException {
		this.file = null;
		this.fileName = MessageUtils.readString(dis);
		this.hash = MessageUtils.readString(dis);
		this.size = dis.readLong();
		this.key = MessageUtils.readString(dis);
		boolean withContentData = dis.readBoolean();
		this.contentData =  withContentData ? new FileContentData(dis) : null;
	}

	@Override
	public FileValueType getType() {
		return FileValueType.STORE_DESCRIPTION;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		return file != null ? new BufferedInputStream(new FileInputStream(file)) : null;
	}

	@Override
	public File getAsFile() {
		try {
			if (file == null) return null;
			Path path = Files.createTempFile("tmp", "." + getFileExtension());
			Files.copy(file.toPath(), path, StandardCopyOption.REPLACE_EXISTING);
			return path.toFile();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
		return key;
	}

	@Override
	public FileContentData getFileContentData() {
		return contentData;
	}

	@Override
	public String getDetectedLanguage() {
		return contentData != null ? contentData.getLanguage() : null;
	}

}
