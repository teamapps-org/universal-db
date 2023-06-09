package org.teamapps.universaldb;

import org.teamapps.universaldb.model.TableModel;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DatabaseManager {

	private final Map<String, DatabaseData> databaseMap = new HashMap<>();
	private final static DatabaseManager BASE_INSTANCE = new DatabaseManager();
	private List<Consumer<UniversalDB>> managedDbHandler = new ArrayList<>();

	public static DatabaseManager getBaseInstance() {
		return BASE_INSTANCE;
	}

	public synchronized void registerDatabase(String name, UniversalDB newDb, ClassLoader classLoader) {
		for (DatabaseData dbData : databaseMap.values()) {
			Set<String> remoteDbNames = dbData.getUniversalDB().getTransactionIndex().getCurrentModel().getRemoteTables().stream().map(TableModel::getRemoteDatabase).collect(Collectors.toSet());
			if (remoteDbNames.contains(newDb.getName()) && remoteDbNames.stream().allMatch(databaseMap::containsKey)) {
				installRemoteTables(dbData.getUniversalDB(), classLoader);
			}
		}
		databaseMap.put(name, new DatabaseData(newDb, classLoader));
		Set<String> remoteDbNames = newDb.getTransactionIndex().getCurrentModel().getRemoteTables().stream().map(TableModel::getRemoteDatabase).collect(Collectors.toSet());
		if (remoteDbNames.stream().allMatch(databaseMap::containsKey)) {
			installRemoteTables(newDb, classLoader);
		}
		managedDbHandler.forEach(handler -> handler.accept(newDb));
	}

	private void installRemoteTables(UniversalDB db, ClassLoader classLoader) {
		db.installRemoteTableClasses(classLoader);
	}

	public void addDatabaseHandler(Consumer<UniversalDB> handler) {
		managedDbHandler.add(handler);
	}

	public synchronized UniversalDB getDatabase(String name) {
		return databaseMap.get(name).getUniversalDB();
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
