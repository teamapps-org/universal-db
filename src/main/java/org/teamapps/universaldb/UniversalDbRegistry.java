package org.teamapps.universaldb;

import org.teamapps.universaldb.model.TableModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UniversalDbRegistry {

	private final Map<String, UniversalDB> universalDBMap = new HashMap<>();
	private final static UniversalDbRegistry INSTANCE = new UniversalDbRegistry();

	public static UniversalDbRegistry getInstance() {
		return INSTANCE;
	}

	public synchronized void registerDatabase(String name, UniversalDB newDb, ClassLoader classLoader) {
		for (UniversalDB db : universalDBMap.values()) {
			Set<String> remoteDbNames = db.getTransactionIndex().getCurrentModel().getRemoteTables().stream().map(TableModel::getRemoteDatabase).collect(Collectors.toSet());
			if (remoteDbNames.contains(newDb.getName()) && remoteDbNames.stream().allMatch(universalDBMap::containsKey)) {
				installRemoteTables(db, classLoader);
			}
		}
		universalDBMap.put(name, newDb);
		Set<String> remoteDbNames = newDb.getTransactionIndex().getCurrentModel().getRemoteTables().stream().map(TableModel::getRemoteDatabase).collect(Collectors.toSet());
		if (remoteDbNames.stream().allMatch(universalDBMap::containsKey)) {
			installRemoteTables(newDb, classLoader);
		}

	}

	private void installRemoteTables(UniversalDB db, ClassLoader classLoader) {
		db.installRemoteTableClasses(classLoader);
	}

	public synchronized UniversalDB getDatabase(String name) {
		return universalDBMap.get(name);
	}


}
