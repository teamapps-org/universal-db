package org.teamapps.universaldb.model;

import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.index.IndexType;

import java.util.HashMap;
import java.util.Map;

public enum FieldType {

	BOOLEAN(1),
	SHORT(2),
	INT(3),
	LONG(4),
	FLOAT(5),
	DOUBLE(6),
	TEXT(7),
	TRANSLATABLE_TEXT(8),
		/*
			languages with 3-letter iso / any language
		 */
	FILE(9),
	SINGLE_REFERENCE(10), //table model
	MULTI_REFERENCE(11), //table model
	TIMESTAMP(12),
	DATE(13),
	TIME(14),
	DATE_TIME(15),
	LOCAL_DATE(16),
	ENUM(17), //enum values
	BINARY(18),
	CURRENCY(19),
	DYNAMIC_CURRENCY(20), //currency list: name, iso, digits
	;

	private final int id;


	FieldType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public boolean isReference() {
		return this == SINGLE_REFERENCE || this == MULTI_REFERENCE;
	}

	public boolean isMultiReference() {
		return this == MULTI_REFERENCE;
	}

	public boolean isEnum() {
		return this == ENUM;
	}

	public boolean isFile() {
		return this == FILE;
	}

	public ColumnType getColumnType() {
		return switch (this){
			case BOOLEAN -> ColumnType.BOOLEAN;
			case SHORT -> ColumnType.SHORT;
			case INT -> ColumnType.INT;
			case LONG -> ColumnType.LONG;
			case FLOAT -> ColumnType.FLOAT;
			case DOUBLE -> ColumnType.DOUBLE;
			case TEXT -> ColumnType.TEXT;
			case TRANSLATABLE_TEXT -> ColumnType.TRANSLATABLE_TEXT;
			case FILE -> ColumnType.FILE;
			case SINGLE_REFERENCE -> ColumnType.SINGLE_REFERENCE;
			case MULTI_REFERENCE -> ColumnType.MULTI_REFERENCE;
			case TIMESTAMP -> ColumnType.TIMESTAMP;
			case DATE -> ColumnType.DATE;
			case TIME -> ColumnType.TIME;
			case DATE_TIME -> ColumnType.DATE_TIME;
			case LOCAL_DATE -> ColumnType.LOCAL_DATE;
			case ENUM -> ColumnType.ENUM;
			case BINARY -> ColumnType.BINARY;
			case CURRENCY -> ColumnType.CURRENCY;
			case DYNAMIC_CURRENCY -> ColumnType.DYNAMIC_CURRENCY;
		};
	}

	public IndexType getIndexType() {
		return switch (this) {
			case BOOLEAN -> IndexType.BOOLEAN;
			case SHORT, ENUM -> IndexType.SHORT;
			case INT, TIMESTAMP, TIME -> IndexType.INT;
			case LONG, DATE_TIME, LOCAL_DATE, CURRENCY, DYNAMIC_CURRENCY, DATE -> IndexType.LONG;
			case FLOAT -> IndexType.FLOAT;
			case DOUBLE -> IndexType.DOUBLE;
			case TEXT -> IndexType.TEXT;
			case TRANSLATABLE_TEXT -> IndexType.TRANSLATABLE_TEXT;
			case FILE -> IndexType.FILE;
			case SINGLE_REFERENCE -> IndexType.REFERENCE;
			case MULTI_REFERENCE -> IndexType.MULTI_REFERENCE;
			case BINARY -> IndexType.BINARY;
		};
	}

	private final static Map<Integer, FieldType> FIELD_TYPE_MAP = new HashMap<>();

	static {
		for (FieldType fieldType : values()) {
			FIELD_TYPE_MAP.put(fieldType.getId(), fieldType);
		}
	}

	public static FieldType getTypeById(int id) {
		return FIELD_TYPE_MAP.get(id);
	}
}
