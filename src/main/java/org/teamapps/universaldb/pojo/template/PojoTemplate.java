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
package org.teamapps.universaldb.pojo.template;

import org.teamapps.universaldb.index.ColumnType;
import org.teamapps.universaldb.index.binary.BinaryFilter;
import org.teamapps.universaldb.index.binary.BinaryIndex;
import org.teamapps.universaldb.index.translation.TranslatableTextFilter;
import org.teamapps.universaldb.index.translation.TranslatableTextIndex;
import org.teamapps.universaldb.schema.Column;
import org.teamapps.universaldb.index.IndexType;
import org.teamapps.universaldb.schema.Table;
import org.teamapps.universaldb.index.bool.BooleanFilter;
import org.teamapps.universaldb.index.bool.BooleanIndex;
import org.teamapps.universaldb.index.file.FileIndex;
import org.teamapps.universaldb.index.numeric.*;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceFilter;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.text.TextFilter;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.generator.PojoCodeGenerator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PojoTemplate {

	private String template;
	private List<String> methods = new ArrayList<>();
	private Map<String, String> blocks = TemplateUtil.getTemplateBlocksMap();

	public static PojoTemplate createSchemaInterface() throws IOException {
		return create(TemplateUtil.SCHEMA_INTERFACE_TPL);
	}

	public static PojoTemplate createEntityInterface() throws IOException {
		return create(TemplateUtil.ENTITY_INTERFACE_TPL);
	}

	public static PojoTemplate createEntityViewInterface() throws IOException {
		return create(TemplateUtil.ENTITY_VIEW_INTERFACE_TPL);
	}

	public static PojoTemplate createUdbEntity() throws IOException {
		return create(TemplateUtil.UDB_ENTITY_TPL);
	}

	public static PojoTemplate createUdbEntityView() throws IOException {
		return create(TemplateUtil.UDB_ENTITY_VIEW_TPL);
	}

	public static PojoTemplate createQueryInterface() throws IOException {
		return create(TemplateUtil.QUERY_INTERFACE_TPL);
	}

	public static PojoTemplate createUdbQuery() throws IOException {
		return create(TemplateUtil.UDB_QUERY_TPL);
	}

	public static PojoTemplate createEnum() throws IOException {
		return create(TemplateUtil.ENUM_TPL);
	}

	public static PojoTemplate create(String name) throws IOException {
		String tpl = TemplateUtil.readeTemplate(name);
		return new PojoTemplate(tpl);
	}

	public PojoTemplate(String template) {
		this.template = template;
	}

	public void setValue(String name, String value) {
		template = TemplateUtil.setValue(template, name, value);
	}

	public String getTemplate() {
		return template;
	}

	public boolean addInterfaceGetMethod(Column column, int version) {
		ColumnType type = column.getType();
		String versionTag = "";
		if (version > 1) {
			versionTag = "_" + version;
		}
		String tpl = blocks.get("INTERFACE_GET_METHOD_" + type.name() + versionTag);
		if (tpl == null) {
			return false;
		}
		tpl = TemplateUtil.setValue(tpl, "name", firstUpper(column.getName()));
		if (type == ColumnType.SINGLE_REFERENCE || type == ColumnType.MULTI_REFERENCE) {
			tpl = TemplateUtil.setValue(tpl, "reference", firstUpper(column.getReferencedTable().getName()));
		} else if (type == ColumnType.ENUM) {
			String enumType = firstUpper(column.getName());
			tpl = TemplateUtil.setValue(tpl, "enum", enumType);
		}
		methods.add(tpl);
		return true;
	}

	public boolean addInterfaceSetMethod(Column column, Table table, int version) {
		ColumnType type = column.getType();
		String versionTag = "";
		if (version > 1) {
			versionTag = "_" + version;
		}
		String tpl = blocks.get("INTERFACE_SET_METHOD_" + type.name() + versionTag);
		if (tpl == null) {
			return false;
		}
		tpl = TemplateUtil.setValue(tpl, "name", firstUpper(column.getName()));
		tpl = TemplateUtil.setValue(tpl, "type", firstUpper(table.getName()));
		if (type == ColumnType.SINGLE_REFERENCE || type == ColumnType.MULTI_REFERENCE) {
			tpl = TemplateUtil.setValue(tpl, "reference", firstUpper(column.getReferencedTable().getName()));
		} else if (type == ColumnType.ENUM) {
			String enumType = firstUpper(column.getName());
			tpl = TemplateUtil.setValue(tpl, "enum", enumType);
		}
		methods.add(tpl);
		return true;
	}


	public boolean addUdbEntityGetMethod(Column column, int version) {
		ColumnType type = column.getType();
		String versionTag = "";
		if (version > 1) {
			versionTag = "_" + version;
		}
		String tpl = blocks.get("ENTITY_GET_METHOD_" + type.name() + versionTag);
		if (tpl == null) {
			return false;
		}
		tpl = TemplateUtil.setValue(tpl, "name", firstUpper(column.getName()));
		tpl = TemplateUtil.setValue(tpl, "name2", column.getName());
		if (type == ColumnType.SINGLE_REFERENCE || type == ColumnType.MULTI_REFERENCE) {
			tpl = TemplateUtil.setValue(tpl, "otherType", firstUpper(column.getReferencedTable().getName()));
			tpl = TemplateUtil.setValue(tpl, "name2", column.getName());
		} else if (type == ColumnType.ENUM) {
			//...
		}
		methods.add(tpl);
		return true;
	}

	public boolean addUdbEntitySetMethod(Column column, Table table, int version) {
		ColumnType type = column.getType();
		String versionTag = "";
		if (version > 1) {
			versionTag = "_" + version;
		}
		String tpl = blocks.get("ENTITY_SET_METHOD_" + type.name() + versionTag);
		if (tpl == null) {
			return false;
		}
		tpl = TemplateUtil.setValue(tpl, "name", firstUpper(column.getName()));
		tpl = TemplateUtil.setValue(tpl, "name2", column.getName());
		tpl = TemplateUtil.setValue(tpl, "type", firstUpper(table.getName()));
		if (type == ColumnType.SINGLE_REFERENCE || type == ColumnType.MULTI_REFERENCE) {
			tpl = TemplateUtil.setValue(tpl, "otherType", firstUpper(column.getReferencedTable().getName()));
			tpl = TemplateUtil.setValue(tpl, "name2", column.getName());
		} else if (type == ColumnType.ENUM) {
			//...
		}
		methods.add(tpl);
		return true;
	}

	public void addQueryInterfaceMethod(Column column, String query, boolean orQuery) {
		String name = orQuery ? "or" + firstUpper(column.getName()) : column.getName();
		if (column.getType() == ColumnType.ENUM) {
			String method = "\t" + query + " " + name + "(EnumFilterType filterType, " +  firstUpper(column.getName()) + " ... enums);";
			methods.add(method);
		} else {
			String method = "\t" + query + " " + name + "(" + getFilterTypeName(column.getType()) + " filter);";
			methods.add(method);
		}
	}

	public void addSubQueryInterfaceMethod(Column column, String query) {
		Table referencedTable = column.getReferencedTable();
		if (column.getType().isReference()) {
			String method = "\t" + query + " filter" + firstUpper(column.getName()) + "(" + firstUpper(column.getReferencedTable().getName() + PojoCodeGenerator.QUERY_SUFFIX) + " query);";
			methods.add(method);
		}
		if (column.getType() == ColumnType.MULTI_REFERENCE) {
			methods.add("\t" + query + " " + column.getName() + "(MultiReferenceFilterType type, " + firstUpper(referencedTable.getName()) + " ... value);");
			methods.add("\t" + query + " " + column.getName() + "Count(MultiReferenceFilterType type, int count);");
		}
	}

	public void addUdbSubQueryMethod(Column column, String query, String type) {
		if (!column.getType().isReference()) {
			return;
		}
		String tpl = blocks.get("QUERY_SUB_QUERY");
		Table referencedTable = column.getReferencedTable();
		String backReference = column.getBackReference();
		if (backReference == null) {
			tpl = blocks.get("QUERY_SUB_QUERY_2");
		}

		String name = firstUpper(column.getName());
		String name2 = column.getName();
		String udbType = PojoCodeGenerator.UDB_PREFIX + type;
		String otherType = firstUpper(referencedTable.getName());

		tpl = TemplateUtil.setValue(tpl, "name", name);
		tpl = TemplateUtil.setValue(tpl, "query", query);
		tpl = TemplateUtil.setValue(tpl, "otherQuery", firstUpper(referencedTable.getName()) + PojoCodeGenerator.QUERY_SUFFIX);
		tpl = TemplateUtil.setValue(tpl, "udbOtherQuery", PojoCodeGenerator.UDB_PREFIX + otherType + PojoCodeGenerator.QUERY_SUFFIX);
		tpl = TemplateUtil.setValue(tpl, "otherUdbType", PojoCodeGenerator.UDB_PREFIX + firstUpper(referencedTable.getName()));
		if (backReference != null) {
			tpl = TemplateUtil.setValue(tpl, "otherName2", backReference);
		}
		tpl = TemplateUtil.setValue(tpl, "name2", name2);
		tpl = TemplateUtil.setValue(tpl, "type", type);
		tpl = TemplateUtil.setValue(tpl, "udbType", udbType);
		methods.add(tpl);

		if (column.getType() == ColumnType.MULTI_REFERENCE) {
			tpl = blocks.get("QUERY_MULTI_REFERENCE");
			tpl = TemplateUtil.setValue(tpl, "otherType", otherType);
			tpl = TemplateUtil.setValue(tpl, "name", name);
			tpl = TemplateUtil.setValue(tpl, "name2", name2);
			tpl = TemplateUtil.setValue(tpl, "query", query);
			tpl = TemplateUtil.setValue(tpl, "udbType", udbType);
			methods.add(tpl);

			tpl = blocks.get("QUERY_MULTI_REFERENCE_2");
			tpl = TemplateUtil.setValue(tpl, "type", type);
			tpl = TemplateUtil.setValue(tpl, "name", name);
			tpl = TemplateUtil.setValue(tpl, "name2", name2);
			tpl = TemplateUtil.setValue(tpl, "query", query);
			tpl = TemplateUtil.setValue(tpl, "udbType", udbType);
			methods.add(tpl);
		}

	}

	public void addUdbQueryMethod(Column column, String query, String type, boolean orQuery) {
		String name = column.getName();
		String tpl = orQuery ? blocks.get("QUERY_METHOD_OR") : blocks.get("QUERY_METHOD");
		if (column.getType() == ColumnType.ENUM) {
			tpl = orQuery ? blocks.get("QUERY_ENUMS_OR") : blocks.get("QUERY_ENUMS");
			tpl = TemplateUtil.setValue(tpl, "enumType", firstUpper(column.getName()));
		}
		tpl = TemplateUtil.setValue(tpl, "query", query);
		tpl = TemplateUtil.setValue(tpl, "name", firstUpper(name));
		tpl = TemplateUtil.setValue(tpl, "name2", name);
		tpl = TemplateUtil.setValue(tpl, "udbType", PojoCodeGenerator.UDB_PREFIX + type);
		tpl = TemplateUtil.setValue(tpl, "filter", getFilterTypeName(column.getType()));
		methods.add(tpl);
	}

	public void addMethod(String method) {
		methods.add(method);
	}


	public String tabs(int count) {
		StringBuilder sb = new StringBuilder();
		sb.append("\t".repeat(count));
		return sb.toString();
	}

	public String nl() {
		return "\n";
	}

	public String firstUpper(String s) {
		return TemplateUtil.firstUpperCase(s);
	}

	public String createConstantName(String s) {
		if (isConstant(s)) {
			return s;
		} else {
			return s.replaceAll("(.)(\\p{Upper})", "$1_$2").toUpperCase();
		}
	}

	private static boolean isConstant(String s) {
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			if (c != '_' && !Character.isUpperCase(c) && !Character.isDigit(c)) {
				return false;
			}
		}
		return true;
	}

	public String getIndexTypeName(ColumnType type) {
		IndexType indexType = type.getIndexType();
		switch (indexType) {
			case BOOLEAN:
				return BooleanIndex.class.getSimpleName();
			case SHORT:
				return ShortIndex.class.getSimpleName();
			case INT:
				return IntegerIndex.class.getSimpleName();
			case LONG:
				return LongIndex.class.getSimpleName();
			case FLOAT:
				return FloatIndex.class.getSimpleName();
			case DOUBLE:
				return DoubleIndex.class.getSimpleName();
			case TEXT:
				return TextIndex.class.getSimpleName();
			case TRANSLATABLE_TEXT:
				return TranslatableTextIndex.class.getSimpleName();
			case REFERENCE:
				return SingleReferenceIndex.class.getSimpleName();
			case MULTI_REFERENCE:
				return MultiReferenceIndex.class.getSimpleName();
			case FILE:
				return FileIndex.class.getSimpleName();
			case BINARY:
				return BinaryIndex.class.getSimpleName();
		}
		return null;
	}

	public String getFilterTypeName(ColumnType type) {
		switch (type) {
			case BOOLEAN:
				return BooleanFilter.class.getSimpleName();
			case SHORT:
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
				return NumericFilter.class.getSimpleName();
			case TEXT:
				return TextFilter.class.getSimpleName();
			case TRANSLATABLE_TEXT:
				return TranslatableTextFilter.class.getSimpleName();
			case FILE:
				return FileFilter.class.getSimpleName();
			case BINARY:
				return BinaryFilter.class.getSimpleName();
			case SINGLE_REFERENCE:
				return NumericFilter.class.getSimpleName();
			case MULTI_REFERENCE:
				return MultiReferenceFilter.class.getSimpleName();
			case TIMESTAMP:
				return NumericFilter.class.getSimpleName();
			case DATE:
				return NumericFilter.class.getSimpleName();
			case TIME:
				return NumericFilter.class.getSimpleName();
			case DATE_TIME:
				return NumericFilter.class.getSimpleName();
			case LOCAL_DATE:
				return NumericFilter.class.getSimpleName();
			case ENUM:
				return NumericFilter.class.getSimpleName();
			case CURRENCY:
				break;
			case DYNAMIC_CURRENCY:
				break;
		}
		return null;
	}

	public void writeTemplate(String name, File dir) throws IOException {
		if (!methods.isEmpty()) {
			StringBuilder sb = new StringBuilder();
			for (String method : methods) {
				sb.append(method).append("\n\n");
			}
			setValue("methods", sb.toString());
			methods.clear();
		}
		File file = new File(dir, TemplateUtil.firstUpperCase(name) + ".java");
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8));
		writer.write(template);
		writer.close();
	}
}
