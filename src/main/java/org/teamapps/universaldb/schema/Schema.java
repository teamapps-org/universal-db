/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2022 TeamApps.org
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
package org.teamapps.universaldb.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.universaldb.TableConfig;
import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.util.DataStreamUtil;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Schema {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final int schemaVersion = 1;
	private String pojoNamespace = "org.teamapps.datamodel";
	private String schemaName = "SchemaInfo";
	private final List<Database> databases = new ArrayList<>();

	public static Schema create() {
		return new Schema();
	}

	public static Schema create(String pojoNamespace) {
		Schema schema = new Schema();
		schema.setPojoNamespace(pojoNamespace);
		return schema;
	}

	public static Schema parse(String schemaData) {
		return new Schema(schemaData);
	}

	public static void checkName(String name) {
		if (name == null || name.isEmpty() || !name.chars().allMatch(Character::isLetterOrDigit)) {
			throw new RuntimeException("ERROR: INVALID NAME:" + name);
		}
	}

	public static void checkColumnName(String name) {
		checkName(name);
		if (Table.isReservedMetaName(name)) {
			throw new RuntimeException("ERROR: FORBIDDEN COLUMN NAME:" + name);
		}
	}

	private static List<String> getTokens(String line) {
		line = line.trim();
		String[] parts = line.split(" ");
		return Arrays.asList(parts);
	}

	public Schema() {
	}

	private Schema(String schemaData) {
		String[] lines = schemaData.split("\n");
		boolean foundSchema = false;
		Database db = null;
		Table table = null;
		Set<String> columnTypes = ColumnType.getNames();
		Map<Column, String> unresolvedReferenceTableMap = new HashMap<>();
		for (String line : lines) {
			int mappingId = 0;
			line = line.trim();
			if (line.startsWith("/") || line.startsWith("#")) {
				continue;
			}
			if (line.endsWith("]")) {
				int start = line.lastIndexOf('[');
				int end = line.lastIndexOf(']');
				mappingId = Integer.parseInt(line.substring(start + 1, end));
				line = line.substring(0, start);
			}
			List<String> tokens = getTokens(line);
			if (tokens.size() < 3 || !tokens.get(1).equalsIgnoreCase("as")) {
				continue;
			}
			String name = tokens.get(0);
			String type = tokens.get(2);
			if (type.equalsIgnoreCase("SCHEMA")) {
				foundSchema = true;
				setPojoNamespace(name);
				if (tokens.size() > 3) {
					String schemaName = tokens.get(3);
					if (!schemaName.isBlank()) {
						setSchemaName(schemaName);
					}
				}
			}
			if (!foundSchema) {
				continue;
			}
			if (type.equalsIgnoreCase("DATABASE")) {
				db = addDatabase(name);
				db.setMappingId(mappingId);
			}
			if (type.equalsIgnoreCase("TABLE")) {
				TableConfig tableConfig = TableConfig.parse(line);
				table = db.addTable(name, tableConfig.getTableOptions());
				table.setMappingId(mappingId);
			}
			if (type.equalsIgnoreCase("VIEW")) {
				String referencedTablePath = tokens.get(4);
				table = db.addView(name, referencedTablePath);
				table.setMappingId(mappingId);
			}
			if (type.equalsIgnoreCase("ENUM")) {
				//enable separate enum definition...
			}
			if (columnTypes.contains(type.toUpperCase()) && !Table.isReservedMetaName(name)) {
				ColumnType columnType = ColumnType.valueOf(type);
				if (table != null) {
					Column column = table.addColumn(name, columnType);
					column.setMappingId(mappingId);
					if (columnType.isReference()) {
						unresolvedReferenceTableMap.put(column, tokens.get(3));
						String backReference = tokens.get(5);
						column.setBackReference(backReference.equals("NONE") ? null : backReference);
						if (line.contains("CASCADE DELETE REFERENCES")) {
							column.setCascadeDeleteReferences(true);
						}
					} else if (columnType == ColumnType.ENUM) {
						line = line.trim();
						String[] values = line.substring(line.indexOf("VALUES (") + 8).replace(")", "").split(", ");
						column.setEnumValues(Arrays.asList(values));
					}
				}
			} else if (columnTypes.contains(type.toUpperCase()) && Table.isReservedMetaName(name)) {
				if (table != null) {
					Column column = table.getColumn(name);
					column.setMappingId(mappingId);
				}
			}
		}
		for (Map.Entry<Column, String> entry : unresolvedReferenceTableMap.entrySet()) {
			Column column = entry.getKey();
			String fullName = entry.getValue();
			String[] parts = fullName.split("\\.");
			String dbName = parts[0];
			String tableName = parts[1];
			Database refDb = getDatabases().stream().filter(database -> database.getName().equals(dbName)).findAny().orElse(null);
			Table referenceTable = refDb.getAllTables().stream().filter(refTable -> refTable.getName().equals(tableName)).findAny().orElse(null);
			column.setReferencedTable(referenceTable);
		}
	}

	public Schema(byte[] data) throws IOException {
		this(new DataInputStream(new ByteArrayInputStream(data)));
	}

	public Schema(DataInputStream dis) throws IOException {
		this(DataStreamUtil.readStringWithLengthHeader(dis));
	}

	public byte[] getSchemaData() {
		String schema = getSchemaDefinition();
		return schema.getBytes(StandardCharsets.UTF_8);
	}

	public void writeSchema(DataOutputStream dataOutputStream) throws IOException {
		byte[] schemaData = getSchemaData();
		DataStreamUtil.writeByteArrayWithLengthHeader(dataOutputStream, schemaData);
	}

	private String createDefinition(boolean ignoreMapping) {
		StringBuilder sb = new StringBuilder();
		sb.append(pojoNamespace).append(" as SCHEMA").append("\n");
		databases.forEach(db -> sb.append(db.createDefinition(ignoreMapping)));
		return sb.toString();
	}

	public List<Database> getDatabases() {
		return databases;
	}

	public Database getDatabase(String name) {
		return databases.stream()
				.filter(db -> db.getName().equals(name))
				.findFirst()
				.orElse(null);
	}

	public Database addDatabase(String name) {
		checkName(name);
		return addDatabase(new Database(this, name));
	}

	private Database addDatabase(Database dataBase) {
		databases.add(dataBase);
		return dataBase;
	}

	public String getPojoNamespace() {
		return pojoNamespace;
	}

	public void setPojoNamespace(String pojoNamespace) {
		this.pojoNamespace = pojoNamespace;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public int getSchemaVersion() {
		return schemaVersion;
	}

	public boolean isCompatibleWith(Schema schema) {
		if (this.schemaVersion != schema.getSchemaVersion()) {
			return false;
		}
		Map<String, Database> databaseMap = databases.stream().collect(Collectors.toMap(Database::getName, db -> db));
		for (Database database : schema.getDatabases()) {
			Database localDatabase = databaseMap.get(database.getName());
			if (localDatabase != null) {
				if (localDatabase.getMappingId() > 0 && database.getMappingId() > 0 && localDatabase.getMappingId() != database.getMappingId()) {
					return false;
				}
				boolean compatibleWith = localDatabase.isCompatibleWith(database);
				if (!compatibleWith) {
					return false;
				}
			} else {
				if (database.getMappingId() != 0 && getMappingIds().contains(database.getMappingId())) {
					return false;
				}
			}
		}
		return true;
	}

	public boolean isSameSchema(Schema schema) throws IOException {
		byte[] schemaData = getSchemaData();
		byte[] schemaData2 = schema.getSchemaData();
		return Arrays.equals(schemaData, schemaData2);
	}

	public boolean isSameSchemaIgnoreMapping(Schema schema) {
		return createDefinition(true).equals(schema.createDefinition(true));
	}

	public void merge(Schema schema) {
		if (!isCompatibleWith(schema)) {
			throw new RuntimeException("Error: cannot merge incompatible schemas:" + this + " with " + schema);
		}
		Map<String, Database> databaseMap = databases.stream().collect(Collectors.toMap(Database::getName, db -> db));
		for (Database database : schema.getDatabases()) {
			Database localDatabase = databaseMap.get(database.getName());
			if (localDatabase == null) {
				addDatabase(database);
			} else {
				if (localDatabase.getMappingId() == 0) {
					localDatabase.setMappingId(database.getMappingId());
				}
				localDatabase.merge(database);
			}
		}
	}

	public boolean checkModel() {
		Set<Integer> mappingIds = new HashSet<>();
		for (Database database : databases) {
			if (database.getMappingId() == 0) {
				logger.error("Missing mapping id:" + database.getFQN());
				return false;
			} else if (mappingIds.contains(database.getMappingId())) {
				logger.error("Duplicate mapping id:" + database.getFQN());
				return false;
			}
			mappingIds.add(database.getMappingId());
			for (Table table : database.getTables()) {
				if (table.getMappingId() == 0) {
					logger.error("Missing mapping id:" + table.getFQN());
					return false;
				} else if (mappingIds.contains(table.getMappingId())) {
					logger.error("Duplicate mapping id:" + table.getFQN());
					return false;
				}
				mappingIds.add(table.getMappingId());
				for (Column columnIndex : table.getColumns()) {
					if (columnIndex.getMappingId() == 0) {
						logger.error("Missing mapping id:" + columnIndex.getFQN());
						return false;
					} else if (mappingIds.contains(columnIndex.getMappingId())) {
						logger.error("Duplicate mapping id:" + columnIndex.getFQN());
						return false;
					}
					mappingIds.add(columnIndex.getMappingId());
				}
			}
		}
		return true;
	}

	public void mapSchema() {
		for (Database database : databases) {
			if (database.getMappingId() == 0) {
				database.setMappingId(getNextMappingId());
			}
			for (Table table : database.getTables()) {
				if (table.getMappingId() == 0) {
					table.setMappingId(getNextMappingId());
				}
				for (Column column : table.getColumns()) {
					if (column.getMappingId() == 0) {
						column.setMappingId(getNextMappingId());
					}
				}
			}
		}
	}

	private int getNextMappingId() {
		Set<Integer> mappingIds = getMappingIds();
		int mappingId = mappingIds.size() + 1;
		while (mappingIds.contains(mappingId)) {
			mappingId++;
		}
		return mappingId;
	}

	public Set<Integer> getMappingIds() {
		Set<Integer> mappingIds = new HashSet<>();
		for (Database database : databases) {
			mappingIds.add(database.getMappingId());
			for (Table table : database.getTables()) {
				mappingIds.add(table.getMappingId());
				for (Column column : table.getColumns()) {
					mappingIds.add(column.getMappingId());
				}
			}
		}
		mappingIds.remove(0);
		return mappingIds;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Schema, version:").append(schemaVersion).append("\n");
		for (Database database : databases) {
			sb.append(database.toString()).append("\n");
		}
		return sb.toString();
	}


	public String getSchemaDefinition() {
		return createDefinition(false);
	}

}
