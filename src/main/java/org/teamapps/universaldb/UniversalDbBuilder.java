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
package org.teamapps.universaldb;

import org.teamapps.universaldb.index.file.store.DatabaseFileStore;
import org.teamapps.universaldb.index.file.store.LocalDatabaseFileStore;
import org.teamapps.universaldb.schema.ModelProvider;

import java.io.File;

public class UniversalDbBuilder {
	private ModelProvider modelProvider;
	private File basePath;
	private String basePathSuffix;
	private File indexPath;
	private File fullTextIndexPath;
	private File transactionLogPath;
	private File fileStorePath;
	private DatabaseFileStore fileStore;
	private DatabaseManager databaseManager;
	private ClassLoader classLoader;
	private boolean skipTransactionIndexCheck = false;

	public static UniversalDbBuilder create() {
		return new UniversalDbBuilder();
	}

	public UniversalDbBuilder() {
	}

	public UniversalDbBuilder basePath(File basePath) {
		this.basePath = basePath;
		return this;
	}

	public UniversalDbBuilder basePathSuffix(String basePathSuffix) {
		this.basePathSuffix = basePathSuffix;
		return this;
	}

	public UniversalDbBuilder indexPath(File indexPath) {
		this.indexPath = indexPath;
		return this;
	}

	public UniversalDbBuilder fullTextIndexPath(File fullTextIndexPath) {
		this.fullTextIndexPath = fullTextIndexPath;
		return this;
	}

	public UniversalDbBuilder transactionLogPath(File transactionLogPath) {
		this.transactionLogPath = transactionLogPath;
		return this;
	}

	public UniversalDbBuilder fileStorePath(File filesPath) {
		this.fileStorePath = filesPath;
		return this;
	}

	public UniversalDbBuilder fileStore(DatabaseFileStore fileStore) {
		this.fileStore = fileStore;
		return this;
	}

	public UniversalDbBuilder modelProvider(ModelProvider modelProvider) {
		this.modelProvider = modelProvider;
		return this;
	}

	public UniversalDbBuilder databaseManager(DatabaseManager databaseManager) {
		this.databaseManager = databaseManager;
		return this;
	}

	public UniversalDbBuilder classLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		return this;
	}

	public UniversalDbBuilder skipTransactionIndexCheck(boolean skipTransactionIndexCheck) {
		this.skipTransactionIndexCheck = skipTransactionIndexCheck;
		return this;
	}

	public UniversalDB build() throws Exception {
		if (basePath != null) {
			if (indexPath == null) {
				indexPath = new File(basePath, "index");
				indexPath.mkdir();
				if (basePathSuffix != null) {
					indexPath = new File(indexPath, basePathSuffix);
					indexPath.mkdir();
				}
			}
			if (fullTextIndexPath == null) {
				fullTextIndexPath = new File(basePath, "text");
				fullTextIndexPath.mkdir();
				if (basePathSuffix != null) {
					fullTextIndexPath = new File(fullTextIndexPath, basePathSuffix);
					fullTextIndexPath.mkdir();
				}
			}
			if (transactionLogPath == null) {
				transactionLogPath = new File(basePath, "transactions");
				transactionLogPath.mkdir();
				if (basePathSuffix != null) {
					transactionLogPath = new File(transactionLogPath, basePathSuffix);
					transactionLogPath.mkdir();
				}
			}
			if (fileStorePath == null && fileStore == null) {
				fileStorePath = new File(basePath, "files");
				fileStorePath.mkdir();
				if (basePathSuffix != null) {
					fileStorePath = new File(fileStorePath, basePathSuffix);
					fileStorePath.mkdir();
				}
			}
		}
		if (fileStore == null) {
			fileStore = new LocalDatabaseFileStore(fileStorePath);
		}
		if (databaseManager == null) {
			databaseManager = DatabaseManager.getBaseInstance();
		}
		if (classLoader == null) {
			classLoader = getClass().getClassLoader();
		}
		return new UniversalDB(modelProvider, databaseManager, fileStore, indexPath, fullTextIndexPath, transactionLogPath, classLoader, skipTransactionIndexCheck);
	}
}
