package org.teamapps.universaldb.message;

import org.teamapps.message.protocol.message.Message;

public class MessagePosition<MESSAGE extends Message> {

	private final int id;
	private final long position;
	private MESSAGE message;

	public MessagePosition(int id, long position) {
		this.id = id;
		this.position = position;
	}

	public int getId() {
		return id;
	}

	public long getPosition() {
		return position;
	}

	public MESSAGE getMessage() {
		return message;
	}

	public void setMessage(MESSAGE message) {
		this.message = message;
	}
}
