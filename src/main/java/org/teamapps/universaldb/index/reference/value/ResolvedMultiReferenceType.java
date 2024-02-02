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
package org.teamapps.universaldb.index.reference.value;

public enum ResolvedMultiReferenceType {
	REMOVE_ALL_REFERENCES(1),
	SET_REFERENCES(2),
	ADD_REMOVE_REFERENCES(3);

	private final int id;

	ResolvedMultiReferenceType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static ResolvedMultiReferenceType getById(int id) {
		switch (id) {
			case 1: return REMOVE_ALL_REFERENCES;
			case 2: return SET_REFERENCES;
			case 3: return ADD_REMOVE_REFERENCES;
		}
		return null;
	}
}
