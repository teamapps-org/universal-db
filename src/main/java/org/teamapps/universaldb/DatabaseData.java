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

public class DatabaseData {

	private final UniversalDB universalDB;
	private ClassLoader classLoader;

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

	public void setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
	}
}
