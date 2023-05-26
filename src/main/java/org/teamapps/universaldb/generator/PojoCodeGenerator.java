/*-
 * ========================LICENSE_START=================================
 * UniversalDB
 * ---
 * Copyright (C) 2014 - 2023 TeamApps.org
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

import org.teamapps.universaldb.model.*;
import org.teamapps.universaldb.pojo.template.PojoTemplate;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PojoCodeGenerator {


	public static final String UDB_PREFIX = "Udb";
	public static final String QUERY_SUFFIX = "Query";

	private static String tabs(int count) {
		return "\t".repeat(count);
	}

	private static String withQuotes(String value) {
		return value != null ? "\"" + value + "\"" : "null";
	}

	private static String withQuotes(List<String> values) {
		return values.stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(", "));
	}

	private static String withBoolean(boolean value) {
		return value ? "true" : "false";
	}

	public void generateCode(DatabaseModel databaseModel, File basePath) throws IOException {
		String namespace = databaseModel.getNamespace();
		File baseDir = createBaseDir(basePath, namespace);
		createModelProviderClass(databaseModel, baseDir);

		File dbPojoDir = new File(baseDir, databaseModel.getName().toLowerCase());
		dbPojoDir.mkdir();
		String packageName = namespace + "." + databaseModel.getName().toLowerCase();

		for (EnumModel enumModel : databaseModel.getEnums()) {
			createEnum(enumModel, dbPojoDir, packageName);
		}

		for (ViewModel viewModel : databaseModel.getViews()) {

		}

		for (TableModel table : databaseModel.getTables()) {
			File dir = dbPojoDir;
			String classPackageName = packageName;
			if (table.isRemoteTable() && table.getRemoteDatabaseNamespace() != null) {
				classPackageName = table.getRemoteDatabaseNamespace() + "." + table.getRemoteDatabase().toLowerCase();
				dir = createBaseDir(basePath, classPackageName);
			}
			createTablePojo(table, dir, classPackageName);
			createTableQueryPojo(table, dir, classPackageName);
		}

	}

	public void createModelProviderClass(DatabaseModel model, File baseDir) throws IOException {
		PojoTemplate tpl = PojoTemplate.createModelProviderClass();
		tpl.setValue("package", model.getNamespace());
		String type = tpl.firstUpper(model.getName());
		StringBuilder sb = new StringBuilder();

		sb.append(tabs(2))
				.append("DatabaseModel model = new DatabaseModel(")
				.append(withQuotes(model.getName())).append(", ")
				.append(withQuotes(model.getTitle())).append(", ")
				.append(withQuotes(model.getNamespace())).append(");")
				.append(tpl.nl());
		sb.append(tabs(2)).append("model.setPojoBuildTime(").append(System.currentTimeMillis()).append("L);").append(tpl.nl());

		for (EnumModel enumModel : model.getEnums()) {
			sb.append(tabs(2))
					.append("model.createEnum(")
					.append(withQuotes(enumModel.getName())).append(", ")
					.append(withQuotes(enumModel.getTitle())).append(", ")
					.append("Arrays.asList(").append(withQuotes(enumModel.getEnumNames())).append("), ")
					.append("Arrays.asList(").append(withQuotes(enumModel.getEnumTitles())).append("));")
					.append(tpl.nl());
		}
		sb.append(tpl.nl());
		for (TableModel table : model.getLocalTables()) {
			sb.append(tabs(2))
					.append("TableModel ")
					.append(table.getName()).append("Table").append(" = ")
					.append("model.createTable(")
					.append(withQuotes(table.getName())).append(", ")
					.append(withQuotes(table.getTitle())).append(", ")
					.append(withBoolean(table.isTrackModifications())).append(", ")
					.append(withBoolean(table.isVersioning())).append(", ")
					.append(withBoolean(table.isRecoverableRecords())).append(");")
					.append(tpl.nl());
		}

		for (TableModel table : model.getRemoteTables()) {
			sb.append(tabs(2))
					.append("TableModel ")
					.append(table.getName()).append("Table").append(" = ")
					.append("model.createRemoteTable(")
					.append(withQuotes(table.getName())).append(", ")
					.append(withQuotes(table.getTitle())).append(", ")
					.append(withQuotes(table.getRemoteDatabase())).append(");")
					.append(tpl.nl());
		}

		for (TableModel table : model.getTables()) {
			sb.append(tpl.nl());
			for (FieldModel field : table.getFields().stream().filter(f -> !f.isMetaField()).toList()) {
				sb.append(tabs(2))
						.append(table.getName()).append("Table.")
						.append(getAddMethodName(field.getFieldType())).append("(")
						.append(withQuotes(field.getName())).append(", ")
						.append(withQuotes(field.getTitle()));
				if (field.getFieldType().isReference()) {
					ReferenceFieldModel referenceFieldModel = (ReferenceFieldModel) field;
					sb.append(", ")
							.append(withQuotes(referenceFieldModel.getReferencedTable().getName())).append(", ")
							.append(withBoolean(referenceFieldModel.isCascadeDelete()));
				} else if (field.getFieldType().isEnum()) {
					EnumFieldModel enumFieldModel = (EnumFieldModel) field;
					sb.append(", ")
							.append(withQuotes(enumFieldModel.getEnumModel().getName()));
				} else if (field.getFieldType().isFile()) {
					FileFieldModel fileFieldModel = (FileFieldModel) field;
					sb.append(", ")
							.append(withBoolean(fileFieldModel.isIndexContent())).append(", ")
							.append(fileFieldModel.getMaxIndexContentLength()).append(", ")
							.append(withBoolean(fileFieldModel.isDetectLanguage()));
				}
				sb.append(");").append(tpl.nl());
			}
		}
		sb.append(tpl.nl());

		for (TableModel table : model.getTables()) {
			for (ReferenceFieldModel referenceField : table.getReferenceFields().stream().filter(rf -> rf.getReverseReferenceField() != null).toList()) {
				sb.append(tabs(2))
						.append("model.addReverseReferenceField(")
						.append(table.getName()).append("Table").append(", ")
						.append(withQuotes(referenceField.getName())).append(", ")
						.append(referenceField.getReferencedTable().getName()).append("Table").append(", ")
						.append(withQuotes(referenceField.getReverseReferenceField().getName()))
						.append(");").append(tpl.nl());
			}
		}

		tpl.setValue("type", type);
		tpl.setValue("model", sb.toString());
		tpl.writeTemplate(type, baseDir);
	}

	private String getAddMethodName(FieldType type) {
		return switch (type) {
			case BOOLEAN -> "addBoolean";
			case SHORT -> "addShort";
			case INT -> "addInteger";
			case LONG -> "addLong";
			case FLOAT -> "addFloat";
			case DOUBLE -> "addDouble";
			case TEXT -> "addText";
			case TRANSLATABLE_TEXT -> "addTranslatableText";
			case FILE -> "addFile";
			case SINGLE_REFERENCE -> "addReference";
			case MULTI_REFERENCE -> "addMultiReference";
			case TIMESTAMP -> "addTimestamp";
			case DATE -> "addDate";
			case TIME -> "addTime";
			case DATE_TIME -> "addDateTime";
			case LOCAL_DATE -> "addLocalDate";
			case ENUM -> "addEnum";
			case BINARY -> "addByteArray";
			case CURRENCY -> "addCurrency";
			case DYNAMIC_CURRENCY -> "addDynamicCurrency";
		};
	}

	private void createEnum(EnumModel enumModel, File dbPojoDir, String packageName) throws IOException {
		PojoTemplate enumTpl = PojoTemplate.createEnum();
		String enumType = enumTpl.firstUpper(enumModel.getName());
		enumTpl.setValue("package", packageName);
		enumTpl.setValue("type", enumType);
		List<String> enumValues = new ArrayList<>();
		for (String enumValue : enumModel.getEnumNames()) {
			enumValues.add("\t" + enumTpl.createConstantName(enumValue) + ",");
		}
		enumTpl.setValue("enumValues", enumValues.stream().collect(Collectors.joining("\n")));
		enumTpl.writeTemplate(enumType, dbPojoDir);
	}

	private void createTablePojo(TableModel table, File dbPojoDir, String packageName) throws IOException {
		PojoTemplate tpl = table.isRemoteTable() ? PojoTemplate.createEntityViewInterface() : PojoTemplate.createEntityInterface();
		PojoTemplate udbTpl = table.isRemoteTable() ? PojoTemplate.createUdbEntityView() : PojoTemplate.createUdbEntity();
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
		staticFields.add("\t" + "protected static UniversalDB universalDB;");

		for (FieldModel fieldModel : table.getFields()) {
			String staticFieldName = "FIELD_" + tpl.createConstantName(fieldModel.getName());
			staticFieldNames.add("\t" + "final static String " + staticFieldName + " = \"" + fieldModel.getName() + "\";");
			staticFields.add("\t" + "protected static " + tpl.getIndexTypeName(fieldModel.getFieldType()) + " " + fieldModel.getName() + ";");
			staticFieldSetters.add("\t\t" + fieldModel.getName() + " = (" + tpl.getIndexTypeName(fieldModel.getFieldType()) + ") tableIndex.getFieldIndex(" + staticFieldName + ");");

			int version = 0;
			boolean loop1 = true;
			boolean loop2 = true;
			boolean loop3 = true;
			boolean loop4 = true;
			while (loop1 || loop2 || loop3 || loop4) {
				version++;
				loop1 = tpl.addInterfaceGetMethod(fieldModel, version);
				if (!table.isRemoteTable()) {
					loop2 = tpl.addInterfaceSetMethod(fieldModel, table, version);
				} else {
					loop2 = false;
				}
				loop3 = udbTpl.addUdbEntityGetMethod(fieldModel, version);
				if (!table.isRemoteTable()) {
					loop4 = udbTpl.addUdbEntitySetMethod(fieldModel, table, version);
				} else {
					loop4 = false;
				}
			}
		}

		tpl.setValue("staticFieldNames", staticFieldNames.stream().collect(Collectors.joining("\n")));
		udbTpl.setValue("staticFields", staticFields.stream().collect(Collectors.joining("\n")));
		udbTpl.setValue("staticFieldSetters", staticFieldSetters.stream().collect(Collectors.joining("\n")));

		tpl.writeTemplate(type, dbPojoDir);
		udbTpl.writeTemplate(udbType, dbPojoDir);
	}

	private void createTableQueryPojo(TableModel table, File dbPojoDir, String packageName) throws IOException {
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

		for (FieldModel fieldModel : table.getFields()) {
			tpl.addSubQueryInterfaceMethod(fieldModel, query);
			tpl.addQueryInterfaceMethod(fieldModel, query, false);
			tpl.addQueryInterfaceMethod(fieldModel, query, true);
			udbTpl.addUdbSubQueryMethod(fieldModel, query, type);
			udbTpl.addUdbQueryMethod(fieldModel, query, type, false);
			udbTpl.addUdbQueryMethod(fieldModel, query, type, true);
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
