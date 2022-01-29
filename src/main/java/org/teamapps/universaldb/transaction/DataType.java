/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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
package org.teamapps.universaldb.transaction;

import java.util.HashMap;
import java.util.Map;

public enum DataType {

	STRING(1),
	INTEGER(2),
	LONG(3),
	BOOLEAN(4),
	FLOAT(5),
	DOUBLE(6),
	SHORT(7),
	BYTE(8),
	BINARY(9),
	FILE_VALUE(10),
	MULTI_REFERENCE(11),

	;

	private int id;
	DataType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	private final static Map<Integer, DataType> dataTypeById = new HashMap<>();

	static {
		for (DataType dataType : values()) {
			dataTypeById.put(dataType.getId(), dataType);
		}

	}

	public static DataType getDataTypeById(int id) {
		return dataTypeById.get(id);
	}
}
