/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2019 TeamApps.org
 * ---
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */
package org.teamapps.universaldb.index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum ColumnType {

	BOOLEAN(1),
	SHORT(2),
	INT(3),
	LONG(4),
	FLOAT(5),
	DOUBLE(6),
	TEXT(7),
	FILE(8),
	SINGLE_REFERENCE(9),
	MULTI_REFERENCE(10),
	TIMESTAMP(11),
	DATE(12),
	TIME(13),
	DATE_TIME(14),
	LOCAL_DATE(15),
	ENUM(16), //todo

	CURRENCY(17), //todo
	DYNAMIC_CURRENCY(18) //todo

	;

	private final int id;

	ColumnType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	private final static Map<Integer, ColumnType> indexTypeById = new HashMap<>();

	static {
		for (ColumnType type : values()) {
			indexTypeById.put(type.getId(), type);
		}

	}

	public static ColumnType getIndexTypeById(int id) {
		return indexTypeById.get(id);
	}

	public boolean isReference() {
		return this == SINGLE_REFERENCE || this == MULTI_REFERENCE;
	}

	public IndexType getIndexType() {
		switch (this) {
			case BOOLEAN:
				return IndexType.BOOLEAN;
			case ENUM:
			case SHORT:
				return IndexType.SHORT;
			case TIMESTAMP:
			case TIME:
			case INT:
				return IndexType.INT;
			case DATE:
			case DATE_TIME:
			case LOCAL_DATE:
			case LONG:
				return IndexType.LONG;
			case FLOAT:
				return IndexType.FLOAT;
			case DOUBLE:
				return IndexType.DOUBLE;
			case TEXT:
				return IndexType.TEXT;
			case FILE:
				return IndexType.FILE;
			case SINGLE_REFERENCE:
				return IndexType.REFERENCE;
			case MULTI_REFERENCE:
				return IndexType.MULTI_REFERENCE;
		}
		return null;
	}

	public static Set<String> getNames() {
		Set<String> nameSet = new HashSet<>();
		for (ColumnType value : values()) {
			nameSet.add(value.name());
		}
		return nameSet;
	}
}
