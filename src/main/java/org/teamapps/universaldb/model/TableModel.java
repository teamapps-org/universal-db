package org.teamapps.universaldb.model;

import org.teamapps.message.protocol.utils.MessageUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableModel {

	public final static String FIELD_CREATION_DATE = "metaCreationDate";
	public final static String FIELD_CREATED_BY = "metaCreatedBy";
	public final static String FIELD_MODIFICATION_DATE = "metaModificationDate";
	public final static String FIELD_MODIFIED_BY = "metaModifiedBy";
	public final static String FIELD_DELETION_DATE = "metaDeletionDate";
	public final static String FIELD_DELETED_BY = "metaDeletedBy";
	public final static String FIELD_RESTORE_DATE = "metaRestoreDate";
	public final static String FIELD_RESTORED_BY = "metaRestoredBy";

	public final static String FIELD_ID = "id";

	public static final String[] FORBIDDEN_COLUMN_NAMES = new String[]{FIELD_CREATION_DATE, FIELD_CREATED_BY, FIELD_MODIFICATION_DATE, FIELD_MODIFIED_BY, FIELD_DELETION_DATE, FIELD_DELETED_BY, FIELD_RESTORE_DATE, FIELD_RESTORED_BY, FIELD_ID, "coll-recs", "coll-del-recs", "versioning-pos", "matches"};

	public static boolean isReservedMetaName(String name) {
		for (String columnName : FORBIDDEN_COLUMN_NAMES) {
			if (name.equals(columnName)) {
				return true;
			}
		}
		return false;
	}


	private final DatabaseModel databaseModel;
	private final String name;
	private final String title;
	private final boolean remoteTable;
	private final String remoteDatabase;
	private final String remoteDatabaseNamespace;
	private final boolean trackModifications;
	private final boolean versioning;
	private final boolean recoverableRecords; //else: record id to index pos column
	private final List<FieldModel> fields = new ArrayList<>();
	private int tableId;
	private boolean deprecated;
	private boolean deleted;
	private int dateCreated;
	private int dateModified;
	private int versionCreated;
	private int versionModified;

	protected TableModel(DatabaseModel databaseModel, String name, String title, boolean remoteTable, String remoteDatabase, String remoteDatabaseNamespace, boolean trackModifications, boolean versioning, boolean recoverableRecords) {
		NamingUtils.checkName(name, title);
		this.databaseModel = databaseModel;
		this.name = NamingUtils.createName(name);
		this.title = NamingUtils.createTitle(title);
		this.remoteTable = remoteTable;
		this.remoteDatabase = remoteDatabase;
		this.remoteDatabaseNamespace = remoteDatabaseNamespace;
		this.trackModifications = trackModifications;
		this.versioning = versioning;
		this.recoverableRecords = recoverableRecords;
		if (trackModifications) {
			addTimestamp(FIELD_CREATION_DATE);
			addInteger(FIELD_CREATED_BY);
			addTimestamp(FIELD_MODIFICATION_DATE);
			addInteger(FIELD_MODIFIED_BY);
		}
		if (recoverableRecords) {
			addTimestamp(FIELD_DELETION_DATE);
			addInteger(FIELD_DELETED_BY);
			addTimestamp(FIELD_RESTORE_DATE);
			addInteger(FIELD_RESTORED_BY);
		}
	}

	protected TableModel(DataInputStream dis, List<Function<DatabaseModel, Boolean>> resolveFunctions, DatabaseModel databaseModel) throws IOException {
		this.databaseModel = databaseModel;
		name = MessageUtils.readString(dis);
		title = MessageUtils.readString(dis);
		remoteTable = dis.readBoolean();
		remoteDatabase = remoteTable ? MessageUtils.readString(dis) : null;
		remoteDatabaseNamespace = remoteTable ? MessageUtils.readString(dis) : null;
		trackModifications = dis.readBoolean();
		versioning = dis.readBoolean();
		recoverableRecords = dis.readBoolean();
		tableId = dis.readInt();
		deprecated = dis.readBoolean();
		deleted = dis.readBoolean();
		dateCreated = dis.readInt();
		dateModified = dis.readInt();
		versionCreated = dis.readInt();
		versionModified = dis.readInt();
		int fieldCount = dis.readInt();
		for (int i = 0; i < fieldCount; i++) {
			FieldType fieldType = FieldType.getTypeById(dis.readInt());
			if (fieldType == FieldType.ENUM) {
				addFieldModel(new EnumFieldModel(dis, this, databaseModel));
			} else if (fieldType == FieldType.SINGLE_REFERENCE || fieldType == FieldType.MULTI_REFERENCE) {
				addFieldModel(new ReferenceFieldModel(dis, this, resolveFunctions));
			} else if (fieldType == FieldType.FILE) {
				addFieldModel(new FileFieldModel(dis, this));
			} else {
				addFieldModel(new FieldModel(dis, this));
			}
		}
	}

	public void write(DataOutputStream dos) throws IOException {
		MessageUtils.writeString(dos, name);
		MessageUtils.writeString(dos, title);
		dos.writeBoolean(remoteTable);
		if (remoteTable) {
			MessageUtils.writeString(dos, remoteDatabase);
			MessageUtils.writeString(dos, remoteDatabaseNamespace);
		}
		dos.writeBoolean(trackModifications);
		dos.writeBoolean(versioning);
		dos.writeBoolean(recoverableRecords);
		dos.writeInt(tableId);
		dos.writeBoolean(deprecated);
		dos.writeBoolean(deleted);
		dos.writeInt(dateCreated);
		dos.writeInt(dateModified);
		dos.writeInt(versionCreated);
		dos.writeInt(versionModified);
		dos.writeInt(fields.size());
		for (FieldModel field : fields) {
			dos.writeInt(field.getFieldType().getId());
			field.write(dos);
		}

	}

	public FieldModel getField(String fieldName) {
		return fields.stream().filter(f -> f.getName().equals(fieldName)).findAny().orElse(null);
	}

	public ReferenceFieldModel getReferenceField(String fieldName) {
		FieldModel fieldModel = fields.stream().filter(f -> f.getName().equals(fieldName)).findAny().orElse(null);
		return fieldModel != null && fieldModel.getFieldType().isReference() ? (ReferenceFieldModel) fieldModel : null;
	}

	public ReferenceFieldModel addReference(String title, TableModel referencedTable) {
		return addReference(title, referencedTable, false);
	}

	public ReferenceFieldModel addReference(String title, TableModel referencedTable, boolean cascadeDelete) {
		return addReference(title, title, referencedTable, cascadeDelete);
	}

	public ReferenceFieldModel addReference(String name, String title, String referencedTable, boolean cascadeDelete) {
		TableModel tableModel = databaseModel.getTable(referencedTable);
		return addReference(name, title, tableModel, cascadeDelete);
	}

	public ReferenceFieldModel addReference(String name, String title, TableModel referencedTable, boolean cascadeDelete) {
		ReferenceFieldModel referenceFieldModel = new ReferenceFieldModel(name, title, this, referencedTable, false, cascadeDelete, null);
		addFieldModel(referenceFieldModel);
		return referenceFieldModel;
	}

	public ReferenceFieldModel addReference(String title, ReferenceFieldModel reverseReference) {
		return addReference(title, reverseReference, false);
	}

	public ReferenceFieldModel addReference(String title, ReferenceFieldModel reverseReference, boolean cascadeDelete) {
		return addReference(title, title, reverseReference, cascadeDelete);
	}

	public ReferenceFieldModel addReference(String name, String title, ReferenceFieldModel reverseReference, boolean cascadeDelete) {
		ReferenceFieldModel referenceFieldModel = new ReferenceFieldModel(name, title, this, reverseReference.getTableModel(), false, cascadeDelete, reverseReference);
		addFieldModel(referenceFieldModel);
		return referenceFieldModel;
	}

	public ReferenceFieldModel addMultiReference(String title, TableModel referencedTable) {
		return addMultiReference(title, referencedTable, false);
	}

	public ReferenceFieldModel addMultiReference(String title, TableModel referencedTable, boolean cascadeDelete) {
		return addMultiReference(title, title, referencedTable, cascadeDelete);
	}

	public ReferenceFieldModel addMultiReference(String name, String title, String referencedTable, boolean cascadeDelete) {
		TableModel tableModel = databaseModel.getTable(referencedTable);
		return addMultiReference(name, title, tableModel, cascadeDelete);
	}

	public ReferenceFieldModel addMultiReference(String name, String title, TableModel referencedTable, boolean cascadeDelete) {
		ReferenceFieldModel referenceFieldModel = new ReferenceFieldModel(name, title, this, referencedTable, true, cascadeDelete, null);
		addFieldModel(referenceFieldModel);
		return referenceFieldModel;
	}

	public ReferenceFieldModel addMultiReference(String title, ReferenceFieldModel reverseReference) {
		return addMultiReference(title, reverseReference, false);
	}

	public ReferenceFieldModel addMultiReference(String title, ReferenceFieldModel reverseReference, boolean cascadeDelete) {
		return addMultiReference(title, title, reverseReference, cascadeDelete);
	}

	public ReferenceFieldModel addMultiReference(String name, String title, ReferenceFieldModel reverseReference, boolean cascadeDelete) {
		ReferenceFieldModel referenceFieldModel = new ReferenceFieldModel(name, title, this, reverseReference.getTableModel(), true, cascadeDelete, reverseReference);
		addFieldModel(referenceFieldModel);
		return referenceFieldModel;
	}

	public EnumFieldModel addEnum(EnumModel enumModel) {
		return addEnum(enumModel.getName(), enumModel.getTitle(), enumModel);
	}

	public EnumFieldModel addEnum(String title, EnumModel enumModel) {
		return addEnum(title, title, enumModel);
	}

	public EnumFieldModel addEnum(String name, String title, String enumName) {
		EnumModel enumModel = databaseModel.getEnumModel(enumName);
		return addEnum(name, title, enumModel);
	}

	public EnumFieldModel addEnum(String name, String title, EnumModel enumModel) {
		EnumFieldModel enumFieldModel = new EnumFieldModel(name, title, this, enumModel);
		addFieldModel(enumFieldModel);
		return enumFieldModel;
	}

	public FileFieldModel addFile(String title) {
		return addFile(title, title, true, true);
	}

	public FileFieldModel addFile(String name, String title) {
		return addFile(name, title, true, true);
	}

	public FileFieldModel addFile(String name, String title, boolean indexContent, boolean detectLanguage) {
		return addFile(name, title, indexContent, 100_000, detectLanguage);
	}

	public FileFieldModel addFile(String name, String title, boolean indexContent, int maxIndexContentLength, boolean detectLanguage) {
		FileFieldModel fileFieldModel = new FileFieldModel(name, title, this, indexContent, maxIndexContentLength, detectLanguage);
		addFieldModel(fileFieldModel);
		return fileFieldModel;
	}

	public FieldModel addBoolean(String title) {
		return addFieldModel(title, FieldType.BOOLEAN);
	}

	public FieldModel addShort(String title) {
		return addFieldModel(title, FieldType.SHORT);
	}

	public FieldModel addInteger(String title) {
		return addFieldModel(title, FieldType.INT);
	}

	public FieldModel addLong(String title) {
		return addFieldModel(title, FieldType.LONG);
	}

	public FieldModel addFloat(String title) {
		return addFieldModel(title, FieldType.FLOAT);
	}

	public FieldModel addDouble(String title) {
		return addFieldModel(title, FieldType.DOUBLE);
	}

	public FieldModel addText(String title) {
		return addFieldModel(title, FieldType.TEXT);
	}

	public FieldModel addTranslatableText(String title) {
		return addFieldModel(title, FieldType.TRANSLATABLE_TEXT);
	}

	public FieldModel addByteArray(String title) {
		return addFieldModel(title, FieldType.BINARY);
	}

	public FieldModel addTimestamp(String title) {
		return addFieldModel(title, FieldType.TIMESTAMP);
	}

	public FieldModel addLocalDate(String title) {
		return addFieldModel(title, FieldType.LOCAL_DATE);
	}

	public FieldModel addDateTime(String title) {
		return addFieldModel(title, FieldType.DATE_TIME);
	}

	public FieldModel addDate(String title) {
		return addFieldModel(title, FieldType.DATE);
	}

	public FieldModel addTime(String title) {
		return addFieldModel(title, FieldType.TIME);
	}

	public FieldModel addBoolean(String name, String title) {
		return addFieldModel(name, title, FieldType.BOOLEAN);
	}

	public FieldModel addShort(String name, String title) {
		return addFieldModel(name, title, FieldType.SHORT);
	}

	public FieldModel addInteger(String name, String title) {
		return addFieldModel(name, title, FieldType.INT);
	}

	public FieldModel addLong(String name, String title) {
		return addFieldModel(name, title, FieldType.LONG);
	}

	public FieldModel addFloat(String name, String title) {
		return addFieldModel(name, title, FieldType.FLOAT);
	}

	public FieldModel addDouble(String name, String title) {
		return addFieldModel(name, title, FieldType.DOUBLE);
	}

	public FieldModel addText(String name, String title) {
		return addFieldModel(name, title, FieldType.TEXT);
	}

	public FieldModel addTranslatableText(String name, String title) {
		return addFieldModel(name, title, FieldType.TRANSLATABLE_TEXT);
	}

	public FieldModel addByteArray(String name, String title) {
		return addFieldModel(name, title, FieldType.BINARY);
	}

	public FieldModel addTimestamp(String name, String title) {
		return addFieldModel(name, title, FieldType.TIMESTAMP);
	}

	public FieldModel addLocalDate(String name, String title) {
		return addFieldModel(name, title, FieldType.LOCAL_DATE);
	}

	public FieldModel addDateTime(String name, String title) {
		return addFieldModel(name, title, FieldType.DATE_TIME);
	}

	public FieldModel addDate(String name, String title) {
		return addFieldModel(name, title, FieldType.DATE);
	}

	public FieldModel addTime(String name, String title) {
		return addFieldModel(name, title, FieldType.TIME);
	}

	private FieldModel addFieldModel(String title, FieldType fieldType) {
		return addFieldModel(title, title, fieldType);
	}

	private FieldModel addFieldModel(String name, String title, FieldType fieldType) {
		FieldModel fieldModel = new FieldModel(name, title, this, fieldType);
		addFieldModel(fieldModel);
		return fieldModel;
	}

	protected FieldModel addFieldModel(FieldModel fieldModel) {
		fields.add(fieldModel);
		return fieldModel;
	}


	public String getName() {
		return name;
	}

	public String getTitle() {
		return title;
	}

	public boolean isRemoteTable() {
		return remoteTable;
	}

	public String getRemoteDatabase() {
		return remoteDatabase;
	}

	public String getRemoteDatabaseNamespace() {
		return remoteDatabaseNamespace;
	}

	public boolean isTrackModifications() {
		return trackModifications;
	}

	public boolean isVersioning() {
		return versioning;
	}

	public boolean isRecoverableRecords() {
		return recoverableRecords;
	}

	public List<FieldModel> getFields() {
		return fields;
	}

	public List<ReferenceFieldModel> getReferenceFields() {
		return fields.stream()
				.filter(f -> f.getFieldType().isReference())
				.map(f -> (ReferenceFieldModel) f)
				.collect(Collectors.toList());
	}

	public List<EnumFieldModel> getEnumFields() {
		return fields.stream()
				.filter(f -> f.getFieldType() == FieldType.ENUM)
				.map(f -> (EnumFieldModel) f)
				.collect(Collectors.toList());
	}

	public List<FileFieldModel> getFileFields() {
		return fields.stream()
				.filter(f -> f.getFieldType() == FieldType.FILE)
				.map(f -> (FileFieldModel) f)
				.collect(Collectors.toList());
	}

	public int getTableId() {
		return tableId;
	}

	protected void setTableId(int tableId) {
		if (this.tableId != 0) {
			throw new RuntimeException("Error: table id already set:" + this.tableId + ", new:" + tableId);
		}
		this.tableId = tableId;
	}

	public boolean isDeprecated() {
		return deprecated;
	}

	protected void setDeprecated(boolean deprecated) {
		this.deprecated = deprecated;
	}

	public boolean isDeleted() {
		return deleted;
	}

	protected void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

	public int getDateCreated() {
		return dateCreated;
	}

	protected void setDateCreated(int dateCreated) {
		this.dateCreated = dateCreated;
	}

	public int getDateModified() {
		return dateModified;
	}

	protected void setDateModified(int dateModified) {
		this.dateModified = dateModified;
	}

	public int getVersionCreated() {
		return versionCreated;
	}

	protected void setVersionCreated(int versionCreated) {
		this.versionCreated = versionCreated;
	}

	public int getVersionModified() {
		return versionModified;
	}

	protected void setVersionModified(int versionModified) {
		this.versionModified = versionModified;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String type = remoteTable ? "remote table (" + remoteDatabase + ")" : "table";
		sb.append(type)
				.append(": ")
				.append(name).append(" (").append(title).append(")")
				.append(" [").append(tableId).append("]\n");
		for (FieldModel field : fields) {
			sb.append("\t").append(field.toString()).append("\n");
		}
		return sb.toString();
	}
}