/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2020 TeamApps.org
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
import java.util.Map;

public enum IndexType {

	BOOLEAN(1),
	SHORT(2),
	INT(3),
	LONG(4),
	FLOAT(5),
	DOUBLE(6),
	TEXT(7),
	TRANSLATABLE_TEXT(8),
	REFERENCE(9),
	MULTI_REFERENCE(10),
	FILE(11),
	BINARY(12),
	
	;

	private final int id;

	IndexType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	private final static Map<Integer, IndexType> indexTypeById = new HashMap<>();

	static {
		for (IndexType indexType : values()) {
			indexTypeById.put(indexType.getId(), indexType);
		}

	}

	public static IndexType getIndexTypeById(int id) {
		return indexTypeById.get(id);
	}

}
