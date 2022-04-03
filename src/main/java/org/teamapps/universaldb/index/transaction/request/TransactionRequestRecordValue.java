package org.teamapps.universaldb.index.transaction.request;

import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.reference.value.MultiReferenceEditValue;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TransactionRequestRecordValue {
	private final int columnId;
	private final IndexType indexType;
	private final Object value;

	public TransactionRequestRecordValue(int columnId, IndexType indexType, Object value) {
		this.columnId = columnId;
		this.indexType = indexType;
		this.value = value;
	}

	public TransactionRequestRecordValue(DataInputStream dis) throws IOException {
		columnId = dis.readInt();
		indexType = IndexType.getIndexTypeById(dis.readByte());
		boolean valueAvailable = dis.readBoolean();
		value = valueAvailable ? readRecordValue(dis) : null;
	}

	public int getColumnId() {
		return columnId;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public Object getValue() {
		return value;
	}

	private Object readRecordValue(DataInputStream dis) throws IOException {
		switch (indexType) {
			case BOOLEAN:
				return dis.readBoolean();
			case SHORT:
				return dis.readShort();
			case INT:
				return dis.readInt();
			case LONG:
				return dis.readLong();
			case FLOAT:
				return dis.readFloat();
			case DOUBLE:
				return dis.readDouble();
			case TEXT:
				return DataStreamUtil.readStringWithLengthHeader(dis);
			case TRANSLATABLE_TEXT:
				return new TranslatableText(dis);
			case REFERENCE:
				return dis.readInt();
			case MULTI_REFERENCE:
				return new MultiReferenceEditValue(dis);
			case FILE:
				return new FileValue(dis);
			case BINARY:
				return DataStreamUtil.readByteArrayWithLengthHeader(dis);
			case FILE_NG:
				break;
		}
		return null;
	}

	public void write(DataOutputStream dos) throws IOException {
		dos.writeInt(columnId);
		dos.writeByte(indexType.getId());
		if (value == null) {
			dos.writeBoolean(false);
		} else {
			dos.writeBoolean(true);
			switch (indexType) {
				case BOOLEAN:
					dos.writeBoolean((Boolean) value);
					break;
				case SHORT:
					dos.writeShort((Short) value);
					break;
				case INT:
					dos.writeInt((Integer) value);
					break;
				case LONG:
					dos.writeLong((Long) value);
					break;
				case FLOAT:
					dos.writeFloat((Float) value);
					break;
				case DOUBLE:
					dos.writeDouble((Double) value);
					break;
				case TEXT:
					DataStreamUtil.writeStringWithLengthHeader(dos, (String) value);
					break;
				case TRANSLATABLE_TEXT:
					TranslatableText translatableText = (TranslatableText) value;
					translatableText.writeValues(dos);
					break;
				case REFERENCE:
					dos.writeInt((Integer) value);
					break;
				case MULTI_REFERENCE:
					MultiReferenceEditValue multiReferenceEditValue = (MultiReferenceEditValue) value;
					multiReferenceEditValue.write(dos);
					break;
				case FILE:
					FileValue fileValue = (FileValue) value;
					fileValue.writeValues(dos);
					break;
				case BINARY:
					DataStreamUtil.writeByteArrayWithLengthHeader(dos, (byte[]) value);
					break;
				case FILE_NG:
					break;
			}
		}
	}
}
