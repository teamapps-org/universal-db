package org.teamapps.universaldb.index.transaction.schema;

import org.teamapps.universaldb.schema.Schema;

import java.io.*;

public class SchemaUpdate {
	private final Schema schema;
	private final long timestamp;
	private final long transactionId;


	public SchemaUpdate(Schema schema, long transactionId, long timestamp) {
		this.schema = schema;
		this.timestamp = timestamp;
		this.transactionId = transactionId;
	}

	public SchemaUpdate(byte[] data) {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
		try {
			schema = new Schema(dis);
			timestamp = dis.readLong();
			transactionId = dis.readLong();
		} catch (IOException e) {
			throw new RuntimeException("Error reading log update", e);
		}
	}

	public byte[] getBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		schema.writeSchema(dos);
		dos.writeLong(timestamp);
		dos.writeLong(transactionId);
		return bos.toByteArray();
	}

	public Schema getSchema() {
		return schema;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public long getTransactionId() {
		return transactionId;
	}
}
