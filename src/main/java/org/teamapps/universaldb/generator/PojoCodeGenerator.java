/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2021 TeamApps.org
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
package org.teamapps.universaldb.generator;

import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.pojo.template.PojoTemplate;
import org.teamapps.universaldb.schema.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PojoCodeGenerator {


	public static final String UDB_PREFIX = "Udb";
	public static final String QUERY_SUFFIX = "Query";

	public void generateCode(Schema schema, File basePath) throws IOException {
		File baseDir = createBaseDir(basePath, schema.getPojoNamespace());
		createSchemaInterface(schema, baseDir);
		for (Database Database : schema.getDatabases()) {
			createDbPojos(Database, baseDir, schema.getPojoNamespace());
		}
	}

	public void createSchemaInterface(Schema schema, File baseDir) throws IOException {
		PojoTemplate tpl = PojoTemplate.createSchemaInterface();
		tpl.setValue("package", schema.getPojoNamespace());
		String[] lines = schema.createDefinition().split("\n");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i];
			if (i > 0) {
				sb.append(tpl.tabs(4));
			}
			if (i + 1 == lines.length) {
				sb.append("\"").append(line).append("\\n\";").append("\n");
			} else {
				sb.append("\"").append(line).append("\\n\" + ").append("\n");
			}
		}
		tpl.setValue("type", schema.getSchemaName());
		tpl.setValue("schema", sb.toString());
		tpl.writeTemplate(schema.getSchemaName(), baseDir);
	}

	private void createDbPojos(Database db, File baseDir, String packageName) throws IOException {
		File dbPojoDir = new File(baseDir, db.getName().toLowerCase());
		dbPojoDir.mkdir();
		for (Table table : db.getAllTables()) {
			createTablePojo(table, dbPojoDir, packageName + "." + db.getName().toLowerCase());
			createTableQueryPojo(table, dbPojoDir, packageName + "." + db.getName().toLowerCase());
		}
	}

	private void createTablePojo(Table table, File dbPojoDir, String packageName) throws IOException {
		PojoTemplate tpl = table.isView() ? PojoTemplate.createEntityViewInterface() : PojoTemplate.createEntityInterface();
		PojoTemplate udbTpl = table.isView() ? PojoTemplate.createUdbEntityView() : PojoTemplate.createUdbEntity();
		String type = tpl.firstUpper(table.getName());
		String udbType = UDB_PREFIX + type;
		String query = type + QUERY_SUFFIX;
		String udbQuery = UDB_PREFIX + query;
		String imports = "";
		tpl.setValue("package", packageName);
		tpl.setValue("type", type);
		tpl.setValue("udbType", udbType);
		tpl.setValue("query", query);
		tpl.setValue("udbQuery", udbQuery);
		tpl.setValue("imports", imports);

		udbTpl.setValue("package", packageName);
		udbTpl.setValue("type", type);
		udbTpl.setValue("udbType", udbType);
		udbTpl.setValue("imports", imports);

		List<String> staticFieldNames = new ArrayList<>();
		List<String> staticFields = new ArrayList<>();
		List<String> staticFieldSetters = new ArrayList<>();
		staticFields.add("\t" + "protected static TableIndex table;");

		for (Column column : table.getColumns()) {
			String staticFieldName = "FIELD_" + tpl.createConstantName(column.getName());
			staticFieldNames.add("\t" + "final static String " + staticFieldName + " = \"" + column.getName() + "\";");
			staticFields.add("\t" + "protected static " + tpl.getIndexTypeName(column.getType()) + " " + column.getName() + ";");
			staticFieldSetters.add("\t\t" + column.getName() + " = (" + tpl.getIndexTypeName(column.getType()) + ") tableIndex.getColumnIndex(" + staticFieldName + ");");

			int version = 0;
			boolean loop1 = true;
			boolean loop2 = true;
			boolean loop3 = true;
			boolean loop4 = true;
			while (loop1 || loop2 || loop3 || loop4) {
				version++;
				loop1 = tpl.addInterfaceGetMethod(column, version);
				if (!table.isView()) {
					loop2 = tpl.addInterfaceSetMethod(column, table, version);
				} else {
					loop2 = false;
				}
				loop3 = udbTpl.addUdbEntityGetMethod(column, version);
				if (!table.isView()) {
					loop4 = udbTpl.addUdbEntitySetMethod(column, table, version);
				} else {
					loop4 = false;
				}
			}

			if (column.getType() == ColumnType.ENUM) {
				PojoTemplate enumTpl = PojoTemplate.createEnum();
				String enumType = enumTpl.firstUpper(column.getName());
				enumTpl.setValue("package", packageName);
				enumTpl.setValue("type", enumType);
				List<String> enumValues = new ArrayList<>();
				for (String enumValue : column.getEnumValues()) {
					enumValues.add("\t" + enumTpl.createConstantName(enumValue) + ",");
				}
				enumTpl.setValue("enumValues", enumValues.stream().collect(Collectors.joining("\n")));
				enumTpl.writeTemplate(enumType, dbPojoDir);
			}
		}

		tpl.setValue("staticFieldNames", staticFieldNames.stream().collect(Collectors.joining("\n")));
		udbTpl.setValue("staticFields", staticFields.stream().collect(Collectors.joining("\n")));
		udbTpl.setValue("staticFieldSetters", staticFieldSetters.stream().collect(Collectors.joining("\n")));

		tpl.writeTemplate(type, dbPojoDir);
		udbTpl.writeTemplate(udbType, dbPojoDir);
	}

	private void createTableQueryPojo(Table table, File dbPojoDir, String packageName) throws IOException {
		PojoTemplate tpl = PojoTemplate.createQueryInterface();
		PojoTemplate udbTpl = PojoTemplate.createUdbQuery();
		String type = tpl.firstUpper(table.getName());
		String udbType = UDB_PREFIX + type;
		String query = type + QUERY_SUFFIX;
		String udbQuery = UDB_PREFIX + query;
		String imports = "";
		tpl.setValue("package", packageName);
		tpl.setValue("type", type);
		tpl.setValue("udbType", udbType);
		tpl.setValue("query", query);
		tpl.setValue("udbQuery", udbQuery);
		tpl.setValue("imports", imports);

		udbTpl.setValue("package", packageName);
		udbTpl.setValue("type", type);
		udbTpl.setValue("udbType", udbType);
		udbTpl.setValue("query", query);
		udbTpl.setValue("udbQuery", udbQuery);
		udbTpl.setValue("imports", imports);

		for (Column column : table.getColumns()) {
			tpl.addSubQueryInterfaceMethod(column, query);
			tpl.addQueryInterfaceMethod(column, query, false);
			tpl.addQueryInterfaceMethod(column, query, true);
			udbTpl.addUdbSubQueryMethod(column, query, type);
			udbTpl.addUdbQueryMethod(column, query, type, false);
			udbTpl.addUdbQueryMethod(column, query, type, true);
		}

		tpl.writeTemplate(query, dbPojoDir);
		udbTpl.writeTemplate(udbQuery, dbPojoDir);
	}

	private File createBaseDir(File basePath, String nameSpace) {
		nameSpace = nameSpace.toLowerCase();
		String[] parts = nameSpace.split("\\.");
		basePath.mkdir();
		File path = basePath;
		for (String part : parts) {
			path = new File(path, part);
			path.mkdir();
		}
		System.out.println("name-space:" + nameSpace + ", path:" + path.getPath());
		return path;
	}

}
