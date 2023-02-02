/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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
package org.teamapps.universaldb.index.transaction.request;

public enum TransactionRequestRecordType {

	CREATE(1),
	CREATE_WITH_ID(2),
	UPDATE(3),
	DELETE(4),
	RESTORE(5);

	private final int id;

	TransactionRequestRecordType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static TransactionRequestRecordType getById(int id) {
		switch (id) {
			case 1: return CREATE;
			case 2: return CREATE_WITH_ID;
			case 3: return UPDATE;
			case 4: return DELETE;
			case 5: return RESTORE;
		}
		return null;
	}
}
