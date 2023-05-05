package org.teamapps.universaldb.index.file.value;

import org.teamapps.message.protocol.utils.MessageUtils;
import org.teamapps.udb.model.FileContentData;
import org.teamapps.universaldb.index.file.FileValue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class StoreDescriptionFile implements FileValue {

	private final String fileName;
	private final long size;
	private final String hash;
	private final String key;
	private final FileContentData contentData;

	public StoreDescriptionFile(String fileName, long size, String hash, String key, FileContentData contentData) {
		this.fileName = fileName;
		this.size = size;
		this.hash = hash;
		this.key = key;
		this.contentData = contentData;
	}

	public StoreDescriptionFile(DataInputStream dis) throws IOException {
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
		throw new RuntimeException("Store description file!");
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
