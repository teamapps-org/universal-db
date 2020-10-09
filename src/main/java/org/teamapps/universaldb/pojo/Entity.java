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
package org.teamapps.universaldb.pojo;

import org.teamapps.universaldb.transaction.Transaction;

import java.util.concurrent.CompletableFuture;

public interface Entity<ENTITY> extends Identifiable{

	int getId();

	void clearChanges();

	boolean isModified();

	default ENTITY save(Transaction transaction) {
		return save(transaction, false);
	}

	ENTITY save(Transaction transaction, boolean strictChangeVerification);

	default ENTITY saveTransactional() {
		return saveTransactional(false);
	}

	ENTITY saveTransactional(boolean strictChangeVerification);

	ENTITY save();

	void delete(Transaction transaction);

	void delete();

	boolean isStored();

	Object getEntityValue(String fieldName);

	void setEntityValue(String fieldName, Object value);

}
