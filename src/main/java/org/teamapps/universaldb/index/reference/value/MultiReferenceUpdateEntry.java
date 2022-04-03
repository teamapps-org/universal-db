package org.teamapps.universaldb.index.reference.value;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

@Deprecated
public class MultiReferenceUpdateEntry {

	private final MultiReferenceUpdateType type;
	private final int referencedRecordId;

	
	public static MultiReferenceUpdateEntry createRemoveAllEntry() {
		return new MultiReferenceUpdateEntry(MultiReferenceUpdateType.REMOVE_ALL, 0);
	}
	
	public static MultiReferenceUpdateEntry createSetEntry(int referencedRecordId) {
		return new MultiReferenceUpdateEntry(MultiReferenceUpdateType.SET, referencedRecordId);
	}

	public static MultiReferenceUpdateEntry createAddEntry(int referencedRecordId) {
		return new MultiReferenceUpdateEntry(MultiReferenceUpdateType.ADD, referencedRecordId);
	}

	public static MultiReferenceUpdateEntry createRemoveEntry(int referencedRecordId) {
		return new MultiReferenceUpdateEntry(MultiReferenceUpdateType.REMOVE, referencedRecordId);
	}

	public static MultiReferenceUpdateEntry readEntry(DataInputStream dis) throws IOException {
		return new MultiReferenceUpdateEntry(dis);
	}

	public MultiReferenceUpdateEntry(MultiReferenceUpdateType type, int referencedRecordId) {
		this.type = type;
		this.referencedRecordId = referencedRecordId;
	}

	public MultiReferenceUpdateEntry(DataInputStream dis) throws IOException {
		this.type = MultiReferenceUpdateType.getById(dis.readByte());
		this.referencedRecordId = dis.readInt();
	}

	public void writeEntry(DataOutputStream dos) throws IOException {
		dos.writeByte(type.getId());
		dos.writeInt(referencedRecordId);
	}

	public MultiReferenceUpdateType getType() {
		return type;
	}

	public int getReferencedRecordId() {
		return referencedRecordId;
	}
}
