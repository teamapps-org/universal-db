/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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

public enum IndexSubType {

	BASE_BOOLEAN(1),
	BASE_SHORT(2),
	BASE_INT(3),
	BASE_LONG(4),
	BASE_FLOAT(5),
	BASE_DOUBLE(6),
	BASE_TEXT(7),
	BASE_MULTI_REFERENCE(8),
	BASE_FILE(9),

	//BOOLEAN:
	BITSET_BOOLEAN(1001),

	//INT:
	TIME_STAMP(3001),
	TIME(3002),
	REFERENCE(3003),

	//LONG:

	;

	private final int id;

	IndexSubType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	private final static Map<Integer, IndexSubType> indexSubTypeById = new HashMap<>();

	static {
		for (IndexSubType indexSubType : values()) {
			indexSubTypeById.put(indexSubType.getId(), indexSubType);
		}

	}

	public static IndexSubType getIndexSubTypeById(int id) {
		return indexSubTypeById.get(id);
	}
}
