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
package org.teamapps.universaldb.index.reference.value;


import java.util.HashMap;
import java.util.Map;

public enum MultiReferenceValueType {

	REFERENCE_ITERATOR(1),
	EDIT_VALUE(2),
	SET_REFERENCES(3),
	ADD_REFERENCES(4),
	REMOVE_REFERENCES(5),
	REMOVE_ALL_REFERENCES(6),

	;

	private final int id;

	MultiReferenceValueType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	private final static Map<Integer, MultiReferenceValueType> multiReferenceValueTypeById = new HashMap<>();

	static {
		for (MultiReferenceValueType multiReferenceValueType : values()) {
			multiReferenceValueTypeById.put(multiReferenceValueType.getId(), multiReferenceValueType);
		}

	}

	public static MultiReferenceValueType getMultiReferenceValueTypeById(int id) {
		return multiReferenceValueTypeById.get(id);
	}
}
