package org.teamapps.universaldb.index.fileng;

import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FileFullTextContentEntry {

	private final int recordId;
	private final int version;
	private final int length;
	private final String contentText;

	public FileFullTextContentEntry(int recordId, int version, int length, String contentText) {
		this.recordId = recordId;
		this.version = version;
		this.length = length;
		this.contentText = contentText;
	}

	public FileFullTextContentEntry(DataInputStream dataInputStream) throws IOException {
		recordId = dataInputStream.readInt();
		version = dataInputStream.readInt();
		length = dataInputStream.readInt();
		contentText = DataStreamUtil.readStringWithLengthHeader(dataInputStream);
	}

	public byte[] getIndexValue() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeInt(recordId);
		dos.writeInt(version);
		dos.writeInt(length);
		DataStreamUtil.writeStringWithLengthHeader(dos, contentText);
		return bos.toByteArray();
	}

	public int getRecordId() {
		return recordId;
	}

	public int getVersion() {
		return version;
	}

	public int getLength() {
		return length;
	}

	public String getContentText() {
		return contentText;
	}
}
