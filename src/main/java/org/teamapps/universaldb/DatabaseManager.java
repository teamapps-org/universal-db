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
package org.teamapps.universaldb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.model.DatabaseModel;
import org.teamapps.universaldb.model.TableModel;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DatabaseManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
	private final static DatabaseManager BASE_INSTANCE = new DatabaseManager();
	private final Map<String, DatabaseData> databaseMap = new HashMap<>();
	private List<Consumer<UniversalDB>> managedDbHandler = new ArrayList<>();

	public static DatabaseManager getBaseInstance() {
		return BASE_INSTANCE;
	}

	public synchronized void registerDatabase(String name, UniversalDB newDb, ClassLoader classLoader) {
		LOGGER.info("Register new database: {}", name);
		databaseMap.put(name, new DatabaseData(newDb, classLoader));
		databaseMap.values().forEach(databaseData -> installRemoteTables(databaseData.getUniversalDB(), databaseData.getClassLoader()));
		managedDbHandler.forEach(handler -> handler.accept(newDb));
	}

	public synchronized void updateDatabase(String name, ClassLoader classLoader) {
		LOGGER.info("Update database: {}", name);
		DatabaseData databaseData = getDatabaseData(name);
		databaseData.setClassLoader(classLoader);
		UniversalDB updatedDb = databaseData.getUniversalDB();
		if (updatedDb == null || classLoader == null) {
			throw new RuntimeException("Error missing database for update:" + name);
		}
		databaseMap.values().forEach(dbData -> installRemoteTables(dbData.getUniversalDB(), dbData.getClassLoader()));
	}

	private void installRemoteTables(UniversalDB db, ClassLoader localDbClassLoader) {
		db.installAvailableRemoteTables(localDbClassLoader);
	}

	public void addDatabaseHandler(Consumer<UniversalDB> handler) {
		managedDbHandler.add(handler);
	}

	public synchronized UniversalDB getDatabase(String name) {
		DatabaseData databaseData = getDatabaseData(name);
		return databaseData != null ? databaseData.getUniversalDB() : null;
	}

	private DatabaseData getDatabaseData(String name) {
		DatabaseData databaseData = databaseMap.get(name);
		return databaseData;
	}

	public synchronized ClassLoader getClassLoader(UniversalDB udb) {
		return getClassLoader(udb.getName());
	}

	public synchronized ClassLoader getClassLoader(String name) {
		return databaseMap.get(name).getClassLoader();
	}

	public synchronized List<UniversalDB> getDatabases() {
		return databaseMap.values().stream().map(DatabaseData::getUniversalDB).toList();
	}


}
