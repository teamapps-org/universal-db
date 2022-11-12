package org.teamapps.universaldb.index.log;

public class IndexMessage {

	private final int id;
	private final long position;
	private byte[] message;

	public IndexMessage(int id, long position) {
		this.id = id;
		this.position = position;
	}

	public int getId() {
		return id;
	}

	public long getPosition() {
		return position;
	}

	public byte[] getMessage() {
		return message;
	}

	public void setMessage(byte[] message) {
		this.message = message;
	}
}
