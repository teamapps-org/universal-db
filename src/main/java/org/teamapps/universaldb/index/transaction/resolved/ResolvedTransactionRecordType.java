/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2025 TeamApps.org
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
package org.teamapps.universaldb.index.transaction.resolved;

import org.teamapps.universaldb.index.transaction.request.TransactionRequestRecordType;
import org.teamapps.universaldb.update.RecordUpdateType;

public enum ResolvedTransactionRecordType {

	CREATE(1),
	CREATE_WITH_ID(2),
	UPDATE(3),
	DELETE(4),
	RESTORE(5),
	ADD_CYCLIC_REFERENCE(6),
	REMOVE_CYCLIC_REFERENCE(7);


	;
	private final int id;

	ResolvedTransactionRecordType(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public static ResolvedTransactionRecordType getById(int id) {
		switch (id) {
			case 1: return CREATE;
			case 2: return CREATE_WITH_ID;
			case 3: return UPDATE;
			case 4: return DELETE;
			case 5: return RESTORE;
			case 6: return ADD_CYCLIC_REFERENCE;
			case 7 : return REMOVE_CYCLIC_REFERENCE;
		}
		return null;
	}

	public static ResolvedTransactionRecordType getByRequestType(TransactionRequestRecordType type) {
		switch (type) {
			case CREATE: return CREATE;
			case CREATE_WITH_ID: return CREATE_WITH_ID;
			case UPDATE: return UPDATE;
			case DELETE: return DELETE;
			case RESTORE: return RESTORE;
		}
		return null;
	}

	public RecordUpdateType getUpdateType() {
		switch (this) {
			case CREATE:
			case CREATE_WITH_ID:
				return RecordUpdateType.CREATE;
			case UPDATE:
			case ADD_CYCLIC_REFERENCE:
			case REMOVE_CYCLIC_REFERENCE:
				return RecordUpdateType.UPDATE;
			case DELETE:
				return RecordUpdateType.DELETE;
			case RESTORE:
				return RecordUpdateType.RESTORE;
		}
		return null;
	}
}
