/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2024 TeamApps.org
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

	BOOLEAN,
	BITSET_BOOLEAN,
	SHORT,
	INT,
	LONG,
	FLOAT,
	DOUBLE,
	TEXT,
	TRANSLATABLE_TEXT,
	FILE,
	SINGLE_REFERENCE,
	MULTI_REFERENCE,
	TIMESTAMP,
	DATE,
	TIME,
	DATE_TIME,
	LOCAL_DATE,
	ENUM,
	BINARY,

	CURRENCY, //todo
	DYNAMIC_CURRENCY //todo

	;


	ColumnType() {
	}


	public boolean isReference() {
		return this == SINGLE_REFERENCE || this == MULTI_REFERENCE;
	}

	public boolean isDateBased() {
		return this == DATE || this == DATE_TIME || this == TIMESTAMP || this == LOCAL_DATE;
	}

	public IndexType getIndexType() {
		switch (this) {
			case BOOLEAN:
			case BITSET_BOOLEAN:
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
			case TRANSLATABLE_TEXT:
				return IndexType.TRANSLATABLE_TEXT;
			case FILE:
				return IndexType.FILE;
			case BINARY:
				return IndexType.BINARY;
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
