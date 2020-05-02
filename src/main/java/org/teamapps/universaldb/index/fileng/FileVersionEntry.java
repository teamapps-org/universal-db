package org.teamapps.universaldb.index.fileng;

import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileVersionEntry {
	private final int version;
	private final long size;
	private final String hash;
	private final String fileName;

	public FileVersionEntry(int version, String hash, String fileName, long size) {
		this.version = version;
		this.hash = hash;
		this.fileName = fileName;
		this.size = size;
	}

	public FileVersionEntry(DataInputStream dataInputStream) throws IOException {
		version = dataInputStream.readInt();
		size = dataInputStream.readLong();
		hash = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
		fileName = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
	}

	public byte[] getEntryValue() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeInt(version);
		dos.writeLong(size);
		DataStreamUtil.writeStringWithLengthHeader(dos, hash);
		DataStreamUtil.writeStringWithLengthHeader(dos, fileName);
		return bos.toByteArray();
	}

	public int getVersion() {
		return version;
	}

	public long getSize() {
		return size;
	}

	public String getHash() {
		return hash;
	}

	public String getFileName() {
		return fileName;
	}
}
