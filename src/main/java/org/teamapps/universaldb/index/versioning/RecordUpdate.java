package org.teamapps.universaldb.index.versioning;

import org.teamapps.universaldb.index.ColumnIndex;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.index.TableIndex;
import org.teamapps.universaldb.index.file.FileValue;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.value.MultiReferenceEditValue;
import org.teamapps.universaldb.index.reference.value.MultiReferenceUpdateEntry;
import org.teamapps.universaldb.index.reference.value.RecordReference;
import org.teamapps.universaldb.index.reference.value.ReferenceIteratorValue;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.schema.Table;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordUpdate {

	private final long previousPosition;
	private final RecordUpdateType type;
	private final int recordId;
	private final int userId;
	private final int timestamp;
	private final long transactionId;
	private final List<RecordUpdateValue> updateValues = new ArrayList<>();

	public RecordUpdate(byte[] bytes) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
		this.previousPosition = dis.readLong();
		this.type = RecordUpdateType.getById(dis.readByte());
		this.recordId = dis.readInt();
		this.userId = dis.readInt();
		this.timestamp = dis.readInt();
		this.transactionId = dis.readLong();
		int size = dis.readInt();
		for (int i = 0; i < size; i++) {
			int columnId = dis.readInt();
			IndexType indexType = IndexType.getIndexTypeById(dis.readByte());
			if (indexType == null) {
				updateValues.add(new RecordUpdateValue(columnId, indexType, null));
			} else {
				switch (indexType) {
					case BOOLEAN:
						updateValues.add(new RecordUpdateValue(columnId, indexType, dis.readBoolean()));
						break;
					case SHORT:
						updateValues.add(new RecordUpdateValue(columnId, indexType, dis.readShort()));
						break;
					case INT:
						updateValues.add(new RecordUpdateValue(columnId, indexType, dis.readInt()));
						break;
					case LONG:
						updateValues.add(new RecordUpdateValue(columnId, indexType, dis.readLong()));
						break;
					case FLOAT:
						updateValues.add(new RecordUpdateValue(columnId, indexType, dis.readFloat()));
						break;
					case DOUBLE:
						updateValues.add(new RecordUpdateValue(columnId, indexType, dis.readDouble()));
						break;
					case TEXT:
						updateValues.add(new RecordUpdateValue(columnId, indexType, DataStreamUtil.readStringWithLengthHeader(dis)));
						break;
					case TRANSLATABLE_TEXT:
						TranslatableText translatableText = new TranslatableText(dis);
						updateValues.add(new RecordUpdateValue(columnId, indexType, translatableText));
						break;
					case REFERENCE:
						updateValues.add(new RecordUpdateValue(columnId, indexType, dis.readInt()));
						break;
					case MULTI_REFERENCE:
						List<MultiReferenceUpdateEntry> updateEntries = new ArrayList<>();
						int count = dis.readInt();
						for (int j = 0; j < count; j++) {
							updateEntries.add(MultiReferenceUpdateEntry.readEntry(dis));
						}
						updateValues.add(new RecordUpdateValue(columnId, indexType, updateEntries));
						break;
					case FILE:
						FileValue fileValue = new FileValue(dis);
						//todo set file supplier!
						//Supplier<File> fileSupplier = fileStore.getFileSupplier(filePath, uuid, hash);
						//fileValue.setFileSupplier(fileSupplier);
						updateValues.add(new RecordUpdateValue(columnId, indexType, fileValue));
						break;
					case BINARY:
						updateValues.add(new RecordUpdateValue(columnId, indexType, DataStreamUtil.readByteArrayWithLengthHeader(dis)));
						break;
					case FILE_NG:
						//todo
						break;
				}
			}
		}
	}

	public static byte[] writeCyclicReference(long previousPosition, CyclicReferenceUpdate cyclicReferenceUpdate, int userId, long timestamp, long transactionId) throws IOException {
		boolean remove = cyclicReferenceUpdate.isRemoveReference();
		ColumnIndex columnIndex = cyclicReferenceUpdate.getReferenceIndex();
		int referencedRecordId = cyclicReferenceUpdate.getReferencedRecordId();

		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		RecordUpdateType recordUpdateType = remove ? RecordUpdateType.REMOVE_CYCLIC_REFERENCE : RecordUpdateType.ADD_CYCLIC_REFERENCE;
		dos.writeLong(previousPosition);
		dos.writeByte(recordUpdateType.getId());
		dos.writeInt(cyclicReferenceUpdate.getRecordId());
		dos.writeInt(userId);
		dos.writeInt((int) (timestamp / 1000));
		dos.writeLong(transactionId);

		dos.writeInt(1);
		dos.writeInt(columnIndex.getMappingId());
		dos.writeByte(columnIndex.getType().getId());

		if (cyclicReferenceUpdate.isSingleReference()) {
			dos.writeInt(referencedRecordId);
		} else {
			List<MultiReferenceUpdateEntry> updateEntries = new ArrayList<>();
			MultiReferenceUpdateEntry entry = remove ? MultiReferenceUpdateEntry.createRemoveEntry(referencedRecordId) : MultiReferenceUpdateEntry.createAddEntry(referencedRecordId);
			updateEntries.add(entry);
			dos.writeInt(updateEntries.size());
			for (MultiReferenceUpdateEntry updateEntry : updateEntries) {
				updateEntry.writeEntry(dos);
			}
			dos.writeInt(referencedRecordId);
		}
		return bos.toByteArray();
	}


	public static byte[] createInitialDeletionUpdate(long previousPosition, int recordId, TableIndex table) throws IOException {
		int userId = 0;
		int timestamp = 0;
		long transactionId = 0; //todo
		ColumnIndex deletionDateColumn = table.getColumnIndex(Table.FIELD_DELETION_DATE);
		ColumnIndex deletedByColumn = table.getColumnIndex(Table.FIELD_DELETED_BY);
		if (deletionDateColumn != null && deletedByColumn != null) {
			userId = (int) deletedByColumn.getGenericValue(recordId);
			timestamp = (int) deletionDateColumn.getGenericValue(recordId);
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		RecordUpdateType recordUpdateType = RecordUpdateType.DELETE;
		dos.writeLong(previousPosition);
		dos.writeByte(recordUpdateType.getId());
		dos.writeInt(recordId);
		dos.writeInt(userId);
		dos.writeInt(timestamp);
		dos.writeLong(transactionId);
		dos.writeInt(0);
		return bos.toByteArray();
	}

	public static byte[] createInitialUpdate(TableIndex table, int recordId) throws IOException {
		int userId = 0;
		int timestamp = 0;
		long transactionId = 0; //todo
		ColumnIndex creationDateColumn = table.getColumnIndex(Table.FIELD_CREATION_DATE);
		ColumnIndex createdByColumn = table.getColumnIndex(Table.FIELD_CREATED_BY);
		if (creationDateColumn != null && createdByColumn != null) {
			userId = (int) createdByColumn.getGenericValue(recordId);
			timestamp = (int) creationDateColumn.getGenericValue(recordId);
		}
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		RecordUpdateType recordUpdateType = RecordUpdateType.CREATE;
		dos.writeLong(0);
		dos.writeByte(recordUpdateType.getId());
		dos.writeInt(recordId);
		dos.writeInt(userId);
		dos.writeInt(timestamp);
		dos.writeLong(transactionId);
		List<ColumnIndex> columnIndices = table.getColumnIndices().stream().filter(col -> !col.isEmpty(recordId)).collect(Collectors.toList());
		dos.writeInt(columnIndices.size());
		for (ColumnIndex column : columnIndices) {
			Object value = column.getGenericValue(recordId);
			dos.writeInt(column.getMappingId());
			dos.writeByte(column.getType().getId());
			switch (column.getType()) {
				case BOOLEAN:
					boolean booleanValue = (boolean) value;
					dos.writeBoolean(booleanValue);
					break;
				case SHORT:
					short shortValue = (short) value;
					dos.writeShort(shortValue);
					break;
				case INT:
					int intValue = (int) value;
					dos.writeInt(intValue);
					break;
				case LONG:
					long longValue = (long) value;
					dos.writeLong(longValue);
					break;
				case FLOAT:
					float floatValue = (float) value;
					dos.writeFloat(floatValue);
					break;
				case DOUBLE:
					double doubleValue = (double) value;
					dos.writeDouble(doubleValue);
					break;
				case TEXT:
					String textValue = (String) value;
					DataStreamUtil.writeStringWithLengthHeader(dos, textValue);
					break;
				case TRANSLATABLE_TEXT:
					TranslatableText translatableTextValue = (TranslatableText) value;
					translatableTextValue.writeValues(dos);
					break;
				case REFERENCE:
					RecordReference referenceValue = (RecordReference) value;
					int referencedId = referenceValue.getRecordId();
					if (referencedId == 0) {
						throw new RuntimeException("Error missing record id for correlation id");
					}
					dos.writeInt(referencedId);
					break;
				case MULTI_REFERENCE:
					ReferenceIteratorValue multiRefValue = (ReferenceIteratorValue) value;
					List<MultiReferenceUpdateEntry> updateEntries = multiRefValue.getAsList().stream().map(MultiReferenceUpdateEntry::createSetEntry).collect(Collectors.toList());
					dos.writeInt(updateEntries.size());
					for (MultiReferenceUpdateEntry updateEntry : updateEntries) {
						updateEntry.writeEntry(dos);
					}
					break;
				case FILE:
					FileValue fileValue = (FileValue) value;
					fileValue.writeValues(dos);
					break;
				case BINARY:
					byte[] bytesValue = (byte[]) value;
					DataStreamUtil.writeByteArrayWithLengthHeader(dos, bytesValue);
					break;
				case FILE_NG:
					//todo
					break;
			}
		}
		return bos.toByteArray();
	}

//	public static byte[] createUpdate(long previousPosition, TransactionRecord transactionRecord, int recordId, int userId, long timestamp, long transactionId, Map<Integer, Integer> recordIdByCorrelationId) throws IOException {
//		ByteArrayOutputStream bos = new ByteArrayOutputStream();
//		DataOutputStream dos = new DataOutputStream(bos);
//		RecordUpdateType recordUpdateType = transactionRecord.getRecordUpdateType();
//		dos.writeLong(previousPosition);
//		dos.writeByte(recordUpdateType.getId());
//		dos.writeInt(recordId);
//		dos.writeInt(userId);
//		dos.writeInt((int) (timestamp / 1000));
//		dos.writeLong(transactionId);
//		List<TransactionRecordValue> recordValues = transactionRecord.getRecordValues();
//		dos.writeInt(recordValues.size());
//		for (TransactionRecordValue recordValue : recordValues) {
//			ColumnIndex column = recordValue.getColumn();
//			Object value = recordValue.getValue();
//			dos.writeInt(column.getMappingId());
//			if (value == null) {
//				dos.writeByte(0);
//			} else {
//				dos.writeByte(column.getType().getId());
//				switch (column.getType()) {
//					case BOOLEAN:
//						boolean booleanValue = (boolean) value;
//						dos.writeBoolean(booleanValue);
//						break;
//					case SHORT:
//						short shortValue = (short) value;
//						dos.writeShort(shortValue);
//						break;
//					case INT:
//						int intValue = (int) value;
//						dos.writeInt(intValue);
//						break;
//					case LONG:
//						long longValue = (long) value;
//						dos.writeLong(longValue);
//						break;
//					case FLOAT:
//						float floatValue = (float) value;
//						dos.writeFloat(floatValue);
//						break;
//					case DOUBLE:
//						double doubleValue = (double) value;
//						dos.writeDouble(doubleValue);
//						break;
//					case TEXT:
//						String textValue = (String) value;
//						DataStreamUtil.writeStringWithLengthHeader(dos, textValue);
//						break;
//					case TRANSLATABLE_TEXT:
//						TranslatableText translatableTextValue = (TranslatableText) value;
//						translatableTextValue.writeValues(dos);
//						break;
//					case REFERENCE:
//						RecordReference referenceValue = (RecordReference) value;
//						referenceValue.updateReference(recordIdByCorrelationId);
//						int referencedId = referenceValue.getRecordId();
//						if (referencedId == 0) {
//							throw new RuntimeException("Error missing record id for correlation id");
//						}
//						dos.writeInt(referencedId);
//						break;
//					case MULTI_REFERENCE:
//						MultiReferenceEditValue multiRefValue = (MultiReferenceEditValue) value;
//						multiRefValue.updateReferences(recordIdByCorrelationId);
//						List<MultiReferenceUpdateEntry> updateEntries = multiRefValue.getResolvedUpdateEntries();
//						dos.writeInt(updateEntries.size());
//						for (MultiReferenceUpdateEntry updateEntry : updateEntries) {
//							updateEntry.writeEntry(dos);
//						}
//						break;
//					case FILE:
//						FileValue fileValue = (FileValue) value;
//						fileValue.writeValues(dos);
//						break;
//					case BINARY:
//						byte[] bytesValue = (byte[]) value;
//						DataStreamUtil.writeByteArrayWithLengthHeader(dos, bytesValue);
//						break;
//					case FILE_NG:
//						//todo
//						break;
//				}
//			}
//		}
//		return bos.toByteArray();
//	}

	public RecordUpdateValue getValue(int columnId) {
		return updateValues.stream()
				.filter(val -> val.getColumnId() == columnId)
				.findFirst().orElse(null);
	}

	public long getPreviousPosition() {
		return previousPosition;
	}

	public RecordUpdateType getType() {
		return type;
	}

	public int getRecordId() {
		return recordId;
	}

	public int getUserId() {
		return userId;
	}

	public int getTimestamp() {
		return timestamp;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public List<RecordUpdateValue> getUpdateValues() {
		return updateValues;
	}
}
