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
package org.teamapps.universaldb.model;

import org.teamapps.message.protocol.message.AttributeType;
import org.teamapps.message.protocol.message.MessageDefinition;
import org.teamapps.message.protocol.message.MessageModelCollection;
import org.teamapps.message.protocol.model.EnumDefinition;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MessageModelConverter {

	public static MessageModelCollection createMessageModel(DatabaseModel dbModel) {
		return createMessageModel(dbModel, "Data");
	}

	public static MessageModelCollection createMessageModel(DatabaseModel dbModel, String dtoSuffix) {
		MessageModelCollection model = new MessageModelCollection(dbModel.getName(), dbModel.getNamespace(), dbModel.getVersion());
		Map<String, EnumDefinition> enumMap = new HashMap<>();
		for (EnumModel dbModelEnum : dbModel.getEnums()) {
			EnumDefinition enumDefinition = model.createEnum(dbModelEnum.getName(), dbModelEnum.getEnumNames());
			enumMap.put(dbModelEnum.getName(), enumDefinition);
		}
		Map<String, MessageDefinition> modelMap = new HashMap<>();

		for (TableModel table : dbModel.getLocalTables()) {
			MessageDefinition def = model.createModel(table.getName() + dtoSuffix);
			modelMap.put(table.getName(), def);
		}

		for (TableModel table : dbModel.getLocalTables()) {
			MessageDefinition def = modelMap.get(table.getName());
			int counter = 1;
			for (FieldModel field : table.getFields()) {
				if (!field.isMetaField()) {
					if (field.getFieldType() == FieldType.MULTI_REFERENCE) {
						def.addMultiReference(field.getName(), counter++, modelMap.get(((ReferenceFieldModel) field).getReferencedTable().getName()));
					} else if (field.getFieldType() == FieldType.SINGLE_REFERENCE) {
						def.addSingleReference(field.getName(), counter++, modelMap.get(((ReferenceFieldModel) field).getReferencedTable().getName()));
					} else if (field.getFieldType() == FieldType.ENUM) {
						def.addEnum(field.getName(), enumMap.get((((EnumFieldModel) field).getEnumModel().getName())), counter++);
					} else {
						def.addAttribute(field.getName(), counter++, getType(field.getFieldType()));
					}
				}
			}

		}
		return model;
	}

	public static String createImportCode(DatabaseModel dbModel, String dtoSuffix) throws IOException {
		String start = "public static void importData(LogIterator logIterator) throws Exception {" +
				"while (logIterator.hasNext()) {\n" +
				"\tbyte[] bytes = logIterator.next();\n" +
				"\tString messageUuid = Message.readMessageUuid(bytes);\n";
		String end = "" +
				"\t}\n" +
				"}\n" +
				"logIterator.close();\n" +
				"}\n";

		StringBuilder sb = new StringBuilder();
		for (TableModel table : dbModel.getLocalTables()) {
			String tableName = table.getName();
			boolean first = sb.isEmpty();
			if (first) {
				sb.append("\tif ");
			} else {
				sb.append("\t} else if ");
			}
			String name = tableName + dtoSuffix;
			sb.append("(data.OBJECT_UUID.equals(messageUuid)) {\n".replace("data", NamingUtils.firstUpperCase( name)));
			sb.append("\t\t").append(NamingUtils.firstUpperCase( tableName)).append(" ").append(tableName).append(" = ").append(NamingUtils.firstUpperCase(tableName)).append(".create();").append("\n");
			sb.append("\t\t").append(NamingUtils.firstUpperCase(name)).append(" ").append(name).append(" = new ").append(NamingUtils.firstUpperCase(name)).append("(bytes);\n");
			for (FieldModel field : table.getFields()) {
				String fieldUpper = NamingUtils.firstUpperCase(field.getName());
				if (!field.isMetaField()) {
					//todo enums and references
					if (field.getFieldType() == FieldType.FILE) {
						sb.append("\t\tif (").append(name).append(".get").append(fieldUpper).append("() != null) ").append(tableName).append(".set").append(fieldUpper).append("(").append(name).append(".get").append(fieldUpper).append("().getAsFile(), ").append(name).append(".get").append(fieldUpper).append("().getFileName());").append("\n");
					} else if (field.getFieldType() == FieldType.BOOLEAN) {
						sb.append("\t\t").append(tableName).append(".set").append(fieldUpper).append("(").append(name).append(".is").append(fieldUpper).append("());").append("\n");
					} else {
						sb.append("\t\t").append(tableName).append(".set").append(fieldUpper).append("(").append(name).append(".get").append(fieldUpper).append("());").append("\n");
					}
				}
			}
			sb.append("\t\t").append("if (").append(name).append(".getRecordCreatedBy() > 0) ").append(tableName).append(".setMetaCreatedBy(").append(name).append(".getRecordCreatedBy());").append("\n");
			sb.append("\t\t").append("if (").append(name).append(".getRecordModifiedBy() > 0) ").append(tableName).append(".setMetaModifiedBy(").append(name).append(".getRecordModifiedBy());").append("\n");
			sb.append("\t\t").append("if (").append(name).append(".getRecordCreationDate() != null) ").append(tableName).append(".setMetaCreationDate(").append(name).append(".getRecordCreationDate());").append("\n");
			sb.append("\t\t").append("if (").append(name).append(".getRecordModificationDate() != null) ").append(tableName).append(".setMetaModificationDate(").append(name).append(".getRecordModificationDate());").append("\n");
			sb.append("\t\t").append(tableName).append(".save();\n");
		}
		return start + sb.toString() + end;
	}

	private static AttributeType getType(FieldType type) {
		return switch (type) {
			case BOOLEAN -> AttributeType.BOOLEAN;
			case SHORT -> AttributeType.INT;
			case INT -> AttributeType.INT;
			case LONG -> AttributeType.LONG;
			case FLOAT -> AttributeType.FLOAT;
			case DOUBLE -> AttributeType.DOUBLE;
			case TEXT -> AttributeType.STRING;
			case TRANSLATABLE_TEXT -> AttributeType.STRING;
			case FILE -> AttributeType.FILE;
			case SINGLE_REFERENCE -> AttributeType.OBJECT_SINGLE_REFERENCE;
			case MULTI_REFERENCE -> AttributeType.OBJECT_MULTI_REFERENCE;
			case TIMESTAMP -> AttributeType.TIMESTAMP_32;
			case DATE -> AttributeType.TIMESTAMP_64;
			case TIME -> AttributeType.TIMESTAMP_64;
			case DATE_TIME -> AttributeType.TIMESTAMP_64;
			case LOCAL_DATE -> AttributeType.DATE;
			case ENUM -> AttributeType.ENUM;
			case BINARY -> AttributeType.BYTE_ARRAY;
			case CURRENCY -> null;
			case DYNAMIC_CURRENCY -> null;
		};

	}

}
