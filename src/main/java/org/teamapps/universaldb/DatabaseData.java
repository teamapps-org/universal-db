package org.teamapps.universaldb;

public class DatabaseData {

	private final UniversalDB universalDB;
	private final ClassLoader classLoader;

	public DatabaseData(UniversalDB universalDB, ClassLoader classLoader) {
		this.universalDB = universalDB;
		this.classLoader = classLoader;
	}

	public DatabaseData(UniversalDB universalDB) {
		this.universalDB = universalDB;
		this.classLoader = this.getClass().getClassLoader();
	}

	public String getName() {
		return universalDB.getName();
	}

	public UniversalDB getUniversalDB() {
		return universalDB;
	}

	public ClassLoader getClassLoader() {
		return classLoader;
	}
}
