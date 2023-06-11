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
package org.teamapps.universaldb.model;

import org.teamapps.commons.util.collections.ByKeyComparisonResult;
import org.teamapps.commons.util.collections.CollectionUtil;
import org.teamapps.message.protocol.utils.MessageUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DatabaseModel {

	private final String name;
	private final String title;
	private final String namespace;
	private final String modelClassName;
	private final List<EnumModel> enums = new ArrayList<>();
	private final List<TableModel> tables = new ArrayList<>();
	private final List<ViewModel> views = new ArrayList<>(); //todo remove!
	private long pojoBuildTime;
	private int version;
	private int dateCreated;
	private int dateModified;


	public DatabaseModel(String title) {
		this(title, title, "org.teamapps.model");
	}

	public DatabaseModel(String name, String title, String namespace) {
		this(name, title, namespace, NamingUtils.createName(name) + "Model");
	}

	public DatabaseModel(String name, String title, String namespace, String modelClassName) {
		this.name = NamingUtils.createName(name);
		this.title = NamingUtils.createTitle(title);
		this.namespace = namespace;
		this.pojoBuildTime = System.currentTimeMillis();
		this.modelClassName = NamingUtils.createName(modelClassName);
	}

	public DatabaseModel(byte[] bytes) throws IOException {
		this(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	public DatabaseModel(DataInputStream dis) throws IOException {
		name = MessageUtils.readString(dis);
		title = MessageUtils.readString(dis);
		namespace = MessageUtils.readString(dis);
		modelClassName = MessageUtils.readString(dis);
		pojoBuildTime = dis.readLong();
		version = dis.readInt();
		dateCreated = dis.readInt();
		dateModified = dis.readInt();
		int enumCount = dis.readInt();
		for (int i = 0; i < enumCount; i++) {
			enums.add(new EnumModel(dis));
		}
		int tableCount = dis.readInt();
		List<Function<DatabaseModel, Boolean>> resolveFunctions = new ArrayList<>();
		for (int i = 0; i < tableCount; i++) {
			TableModel tableModel = new TableModel(dis, resolveFunctions, this);
			tables.add(tableModel);
		}
		for (Function<DatabaseModel, Boolean> resolveFunction : resolveFunctions) {
			if (!resolveFunction.apply(this)) {
				throw new RuntimeException("Db model mapping error");
			}
		}
		int views = dis.readInt();
	}

	public void write(DataOutputStream dos) throws IOException {
		MessageUtils.writeString(dos, name);
		MessageUtils.writeString(dos, title);
		MessageUtils.writeString(dos, namespace);
		MessageUtils.writeString(dos, modelClassName);
		dos.writeLong(pojoBuildTime);
		dos.writeInt(version);
		dos.writeInt(dateCreated);
		dos.writeInt(dateModified);
		dos.writeInt(enums.size());
		for (EnumModel enumModel : enums) {
			enumModel.write(dos);
		}
		dos.writeInt(tables.size());
		for (TableModel table : tables) {
			table.write(dos);
		}
		dos.writeInt(views.size());
		for (ViewModel view : views) {
			view.write(dos);
		}
	}

	public void initialize() {
		if (version != 0) {
			return;
		}
		version = 1;
		int timestamp = (int) (System.currentTimeMillis() / 1000);
		dateCreated = timestamp;
		AtomicInteger idGenerator = new AtomicInteger();
		for (EnumModel enumModel : enums) {
			enumModel.setVersionCreated(version);
			enumModel.setDateCreated(timestamp);
		}

		for (TableModel table : tables) {
			table.setTableId(idGenerator.incrementAndGet());
			table.setVersionCreated(version);
			table.setDateCreated(timestamp);
			for (FieldModel field : table.getFields()) {
				field.setFieldId(idGenerator.incrementAndGet());
				field.setVersionCreated(version);
				field.setDateCreated(timestamp);
			}
		}
		for (ViewModel view : views) {
			view.setVersionCreated(version);
			view.setDateCreated(timestamp);
			//todo add view fields with meta data!
		}
	}

	public void mergeModel(DatabaseModel model) {
		if (!model.isValid()) {
			throw new RuntimeException("Invalid model for merge");
		}
		if (!isCompatible(model)) {
			throw new RuntimeException("Incompatible model for merge");
		}
		int version = getVersion() + 1;
		int timestamp = (int) (System.currentTimeMillis() / 1000);
		setVersion(version);
		setDateModified(timestamp);
		AtomicInteger idGenerator = new AtomicInteger(getMaxId());

		ByKeyComparisonResult<EnumModel, EnumModel, String> enumCompare = CollectionUtil.compareByKey(enums, model.getEnums(), EnumModel::getName, EnumModel::getName, true);
		//existing enums
		for (EnumModel enumModel : enumCompare.getBEntriesInA()) {
			EnumModel existingEnum = enumCompare.getA(enumModel);
			if (enumModel.getEnumNames().size() > existingEnum.getEnumNames().size()) {
				existingEnum.updateValues(enumModel.getEnumNames(), enumModel.getEnumTitles());
				existingEnum.setVersionModified(version);
				existingEnum.setDateModified(version);
			}
		}

		//new enums
		for (EnumModel enumModel : enumCompare.getBEntriesNotInA()) {
			enumModel.setVersionCreated(version);
			enumModel.setDateCreated(timestamp);
			addEnum(enumModel);
		}

		//removed enums
		for (EnumModel existingEnum : enumCompare.getAEntriesNotInB().stream().filter(e -> !e.isDeleted() && !e.isDeprecated()).toList()) {
			existingEnum.setDeprecated(true);
			existingEnum.setVersionModified(version);
			existingEnum.setDateModified(timestamp);
		}

		ByKeyComparisonResult<TableModel, TableModel, String> tableCompare = CollectionUtil.compareByKey(getTables(), model.getTables(), TableModel::getName, TableModel::getName, true);
		//existing tables
		for (TableModel table : tableCompare.getBEntriesInA()) {
			TableModel existingTable = tableCompare.getA(table);

			ByKeyComparisonResult<FieldModel, FieldModel, String> fieldResult = CollectionUtil.compareByKey(existingTable.getFields(), table.getFields(), FieldModel::getName, FieldModel::getName, true);
			//existing fields
			for (FieldModel field : fieldResult.getBEntriesInA()) {
				boolean fieldIsUpdated = false;
				FieldModel existingField = fieldResult.getA(field);
				if (existingField.getTitle().equals(field.getTitle())) {
					existingField.setTitle(field.getTitle());
					fieldIsUpdated = true;
				}
				if (fieldIsUpdated) {
					field.setVersionModified(version);
					field.setDateModified(timestamp);
				}
			}

			//new fields
			for (FieldModel field : fieldResult.getBEntriesNotInA()) {
				table.addFieldModel(field);
				if (field.getFieldId() == 0) {
					field.setFieldId(idGenerator.incrementAndGet());
					field.setVersionCreated(version);
					field.setDateCreated(timestamp);
				}
			}

			//removed fields
			for (FieldModel existingField : fieldResult.getAEntriesNotInB().stream().filter(f -> !f.isDeleted() && !f.isDeprecated()).toList()) {
				existingField.setDeprecated(true);
				existingField.setVersionModified(version);
				existingField.setDateModified(timestamp);
			}
		}

		//new tables
		for (TableModel table : tableCompare.getBEntriesNotInA()) {
			addTable(table);
			if (table.getTableId() == 0) {
				table.setTableId(idGenerator.incrementAndGet());
				table.setVersionCreated(version);
				table.setDateCreated(timestamp);
			}
		}

		//removed tables
		for (TableModel existingTable : tableCompare.getAEntriesNotInB().stream().filter(t -> !t.isDeleted() && !t.isDeprecated()).toList()) {
			existingTable.setDeprecated(true);
			existingTable.setVersionModified(version);
			existingTable.setDateModified(timestamp);
		}

	}

	public boolean isCompatible(DatabaseModel model) {
		return checkCompatibilityErrors(model).isEmpty();
	}

	public List<String> checkCompatibilityErrors(DatabaseModel newModel) {
		List<String> errors = new ArrayList<>();
		if (!newModel.isValid()) {
			errors.add("Model not valid!");
		}
		if (!name.equals(newModel.getName())) {
			errors.add("Wrong model name: " + name + " vs: " + newModel.getName());
		}

		ByKeyComparisonResult<EnumModel, EnumModel, String> enumCompare = CollectionUtil.compareByKey(enums, newModel.getEnums(), EnumModel::getName, EnumModel::getName, true);
		for (EnumModel enumModel : enumCompare.getBEntriesInA()) {
			EnumModel existingEnum = enumCompare.getA(enumModel);
			if (existingEnum.getEnumNames().size() > enumModel.getEnumNames().size()) {
				errors.add("Wrong config for enum " + enumModel.getName());
			} else {
				for (int i = 0; i < existingEnum.getEnumNames().size(); i++) {
					if (!existingEnum.getEnumNames().get(i).equals(enumModel.getEnumNames().get(i))) {
						errors.add("Wrong config for enum " + enumModel.getName());
					}
				}
			}
		}

		for (TableModel table : newModel.getTables()) {
			TableModel existingTable = getTable(table.getName());
			if (existingTable != null) {
				if (existingTable.isVersioning() != table.isVersioning() ||
						existingTable.isRecoverableRecords() != table.isRecoverableRecords() ||
						existingTable.isTrackModifications() != table.isTrackModifications() ||
						existingTable.isRemoteTable() != table.isRemoteTable() ||
						(existingTable.isRemoteTable() && !existingTable.getRemoteDatabase().equals(table.getRemoteDatabase()))
				) {
					errors.add("Wrong config for table " + table.getName());
				}
				for (FieldModel field : table.getFields()) {
					FieldModel existingField = existingTable.getField(field.getName());
					if (existingField != null) {
						if (existingField.getFieldType() != field.getFieldType()) {
							errors.add("Wrong config for field " + field.getName());
						} else {
							if (existingField.getFieldType().isReference()) {
								ReferenceFieldModel existingReference = (ReferenceFieldModel) existingField;
								ReferenceFieldModel referenceField = (ReferenceFieldModel) field;
								if (existingReference.isMultiReference() != referenceField.isMultiReference() ||
										existingReference.isCascadeDelete() != referenceField.isCascadeDelete() ||
										!existingReference.getReferencedTable().getName().equals(referenceField.getReferencedTable().getName()) ||
										Objects.isNull(existingReference.getReverseReferenceField()) != Objects.isNull(referenceField.getReverseReferenceField()) ||
										(existingReference.getReverseReferenceField() != null && !existingReference.getReverseReferenceField().getName().equals(referenceField.getReverseReferenceField().getName()))
								) {
									errors.add("Wrong config for field " + field.getName());
								}
							} else if (existingField.getFieldType() == FieldType.ENUM) {
								EnumFieldModel existingEnum = (EnumFieldModel) existingField;
								EnumFieldModel enumField = (EnumFieldModel) field;
								if (!existingEnum.getEnumModel().getName().equals(enumField.getEnumModel().getName())) {
									errors.add("Wrong enum model for field " + field.getName());
								}
							}
						}
					}
				}
			}
		}

		ByKeyComparisonResult<ViewModel, ViewModel, String> viewCompare = CollectionUtil.compareByKey(views, newModel.getViews(), ViewModel::getName, ViewModel::getName, true);
		for (ViewModel viewModel : viewCompare.getBEntriesInA()) {
			ViewModel existingView = viewCompare.getA(viewModel);
			if (!existingView.getTable().getName().equals(viewModel.getTable().getName())) {
				errors.add("Wrong table for view " + viewModel.getName());
			}
		}
		return errors;
	}

	public boolean isValid() {
		return checkErrors().isEmpty();
	}

	public List<String> checkErrors() {
		List<String> errors = new ArrayList<>();
		Set<String> tableNames = new HashSet<>();

		enums.stream()
				.collect(Collectors.groupingBy(f -> f.getName().toLowerCase(), Collectors.counting()))
				.entrySet().stream()
				.filter(e -> e.getValue() > 1)
				.map(Map.Entry::getKey)
				.forEach(name -> errors.add("Duplicate enum name:" + name));

		tables.stream()
				.collect(Collectors.groupingBy(f -> f.getName().toLowerCase(), Collectors.counting()))
				.entrySet().stream()
				.filter(e -> e.getValue() > 1)
				.map(Map.Entry::getKey)
				.forEach(name -> errors.add("Duplicate table name:" + name));

		views.stream()
				.collect(Collectors.groupingBy(f -> f.getName().toLowerCase(), Collectors.counting()))
				.entrySet().stream()
				.filter(e -> e.getValue() > 1)
				.map(Map.Entry::getKey)
				.forEach(name -> errors.add("Duplicate view name:" + name));

		for (EnumModel enumModel : enums) {
			if (enumModel.getEnumNames().isEmpty() || enumModel.getEnumNames().size() != enumModel.getEnumTitles().size()) {
				errors.add("Wrong enum values:" + enumModel.getName());
			}
			if (enumModel.getEnumNames().stream().anyMatch(v -> v == null || v.strip().isBlank())) {
				errors.add("Empty enum value:" + enumModel.getName());
			}
			if (enumModel.getEnumTitles().stream().anyMatch(v -> v == null || v.strip().isBlank())) {
				errors.add("Empty enum title:" + enumModel.getName());
			}
		}

		for (TableModel table : tables) {
			if (tableNames.contains(table.getName().toLowerCase())) {
				errors.add("Duplicate table name:" + table.getName());
			}
			tableNames.add(table.getName().toLowerCase());
			table.getFields().stream()
					.collect(Collectors.groupingBy(f -> f.getName().toLowerCase(), Collectors.counting()))
					.entrySet().stream()
					.filter(e -> e.getValue() > 1)
					.map(Map.Entry::getKey)
					.forEach(fieldName -> errors.add("Duplicate field name '" + fieldName + "' in table " + table.getName()));

			table.getReferenceFields().stream()
					.filter(f -> f.getReverseReferenceField() != null)
					.filter(f -> !f.equals(f.getReverseReferenceField().getReverseReferenceField()))
					.forEach(f -> errors.add("Wrong reverse reference field " + f.getName() + " in table " + table.getName()));

			table.getEnumFields().stream()
					.filter(f -> getEnumModel(f.getEnumModel().getName()) == null)
					.forEach(f -> errors.add("Wrong enum field " + f.getName() + " in table " + table.getName()));

		}
		for (ViewModel view : views) {
			if (tableNames.contains(view.getName().toLowerCase())) {
				errors.add("Duplicate table/view name:" + view.getName());
			}
			tableNames.add(view.getName().toLowerCase());
			view.getFields().stream()
					.collect(Collectors.groupingBy(f -> f.getName().toLowerCase(), Collectors.counting()))
					.entrySet().stream()
					.filter(e -> e.getValue() > 1)
					.map(Map.Entry::getKey)
					.forEach(fieldName -> errors.add("Duplicate field " + fieldName + " in view " + view.getName()));
			String tableName = view.getTable().getName();
			view.getFields().stream()
					.filter(f -> !f.getTableModel().getName().equals(tableName))
					.forEach(f -> errors.add("View with field from other table, field:" + f.getName() + ", view:" + view.getName()));
		}
		return errors;
	}

	public boolean checkIds() {
		for (TableModel table : tables) {
			if (table.getTableId() <= 0) return false;
			for (FieldModel field : table.getFields()) {
				if (field.getFieldId() <= 0) return false;
			}
		}
		return true;
	}

	public boolean isSameModel(DatabaseModel model) {
		if (!name.equals(model.getName())) return false;
		if (!title.equals(model.getTitle())) return false;
		if (!namespace.equals(model.getNamespace())) return false;
		ByKeyComparisonResult<EnumModel, EnumModel, String> enumCompare = CollectionUtil.compareByKey(enums, model.getEnums(), EnumModel::getName, EnumModel::getName, true);
		if (enumCompare.isDifferent()) return false;
		for (EnumModel enumModel : enumCompare.getBEntriesInA()) {
			EnumModel existingModel = enumCompare.getA(enumModel);
			if (!existingModel.getName().equals(enumModel.getName())) return false;
			if (!existingModel.getTitle().equals(enumModel.getTitle())) return false;
			if (CollectionUtil.compareByKey(existingModel.getEnumNames(), enumModel.getEnumNames(), s -> s, s -> s).isDifferent()) return true;
			if (CollectionUtil.compareByKey(existingModel.getEnumTitles(), enumModel.getEnumTitles(), s -> s, s -> s).isDifferent()) return true;
		}
		ByKeyComparisonResult<TableModel, TableModel, String> tableCompare = CollectionUtil.compareByKey(tables, model.getTables(), TableModel::getName, TableModel::getName, true);
		if (tableCompare.isDifferent()) return false;
		for (TableModel tableModel : tableCompare.getBEntriesInA()) {
			TableModel existingModel = tableCompare.getA(tableModel);
			if (!existingModel.getName().equals(tableModel.getName())) return false;
			if (!existingModel.getTitle().equals(tableModel.getTitle())) return false;
			if (existingModel.isRemoteTable() != tableModel.isRemoteTable()) return false;
			if (existingModel.isTrackModifications() != tableModel.isTrackModifications()) return false;
			if (existingModel.isVersioning() != tableModel.isVersioning()) return false;
			if (existingModel.isRecoverableRecords() != tableModel.isRecoverableRecords()) return false;
			if (existingModel.getRemoteDatabase() != null && !existingModel.getRemoteDatabase().equals(tableModel.getRemoteDatabase())) return false;
			ByKeyComparisonResult<FieldModel, FieldModel, String> fieldCompare = CollectionUtil.compareByKey(existingModel.getFields(), tableModel.getFields(), FieldModel::getName, FieldModel::getName, true);
			if (fieldCompare.isDifferent()) return false;
			for (FieldModel fieldModel : fieldCompare.getBEntriesInA()) {
				FieldModel existingField = fieldCompare.getA(fieldModel);
				if (!existingField.getName().equals(fieldModel.getName())) return false;
				if (!existingField.getTitle().equals(fieldModel.getTitle())) return false;
				if (existingField.getFieldType() != fieldModel.getFieldType()) return false;
				if (fieldModel.getFieldType().isFile()) {
					FileFieldModel fileFieldModel = (FileFieldModel) fieldModel;
					FileFieldModel existingFileModel = (FileFieldModel) existingField;
					if (existingFileModel.isIndexContent() != fileFieldModel.isIndexContent()) return false;
					if (existingFileModel.isDetectLanguage() != fileFieldModel.isDetectLanguage()) return false;
					if (existingFileModel.getMaxIndexContentLength() != fileFieldModel.getMaxIndexContentLength()) return false;
				} else if (fieldModel.getFieldType().isReference()) {
					ReferenceFieldModel referenceFieldModel = (ReferenceFieldModel) fieldModel;
					ReferenceFieldModel existingReferenceModel = (ReferenceFieldModel) existingField;
					if (existingReferenceModel.isMultiReference() != referenceFieldModel.isMultiReference()) return false;
					if (existingReferenceModel.isCascadeDelete() != referenceFieldModel.isCascadeDelete()) return false;
					if (!existingReferenceModel.getReferencedTable().getName().equals(referenceFieldModel.getReferencedTable().getName())) return false;
					if ((existingReferenceModel.getReverseReferenceField() == null) != (referenceFieldModel.getReverseReferenceField() == null)) return false;
					if (existingReferenceModel.getReverseReferenceField() != null && !existingReferenceModel.getReverseReferenceField().getName().equals(referenceFieldModel.getReverseReferenceField().getName())) return false;
				}
			}

		}
		return true;
	}

	private int getMaxId() {
		int tableId = tables.stream().mapToInt(TableModel::getTableId).max().orElse(0);
		int fieldId = tables.stream().flatMap(t -> t.getFields().stream()).mapToInt(FieldModel::getFieldId).max().orElse(0);
		return Math.max(tableId, fieldId);
	}


	public TableModel createTable(String title) {
		return createTable(title, title, true, true, true);
	}

	public TableModel createTable(String name, String title) {
		return createTable(name, title, true, true, true);
	}

	public TableModel createTable(String name, String title, boolean trackModifications, boolean versioning, boolean recoverableRecords) {
		TableModel tableModel = new TableModel(this, name, title, false, null, null, trackModifications, versioning, recoverableRecords);
		return addTable(tableModel);
	}

	public TableModel createRemoteTable(String title, String databaseName) {
		return createRemoteTable(title, title, databaseName);
	}

	public TableModel createRemoteTable(String name, String title, String databaseName) {
		TableModel tableModel = new TableModel(this, name, title, true, databaseName, null, false, false, false);
		return addTable(tableModel);
	}

	public TableModel createRemoteTable(String name, String title, String databaseName, String namespace) {
		TableModel tableModel = new TableModel(this, name, title, true, databaseName, namespace, false, false, false);
		return addTable(tableModel);
	}

	public EnumModel createEnum(String title, String... values) {
		return createEnum(title, Arrays.asList(values));
	}

	public EnumModel createEnum(String title, List<String> enumTitles) {
		return createEnum(title, title, enumTitles, enumTitles);
	}

	public EnumModel createEnum(String name, String title, List<String> enumTitles) {
		return createEnum(name, title, enumTitles, enumTitles);
	}

	public EnumModel createEnum(String name, String title, List<String> enumNames, List<String> enumTitles) {
		EnumModel enumModel = new EnumModel(name, title, enumNames, enumTitles);
		return addEnum(enumModel);
	}

	private EnumModel addEnum(EnumModel enumModel) {
		if (enums.stream().anyMatch(e -> e.getName().equalsIgnoreCase(enumModel.getName()))) {
			throw new RuntimeException("Error: enum with name " + enumModel.getName() + " already exists!");
		}
		enums.add(enumModel);
		return enumModel;
	}

	public EnumModel getEnumModel(String enumModelName) {
		return enums.stream()
				.filter(e -> e.getName().equals(enumModelName))
				.findAny()
				.orElseThrow();
	}

	public List<EnumModel> getEnums() {
		return new ArrayList<>(enums);
	}

	private TableModel addTable(TableModel tableModel) {
		if (tables.stream().anyMatch(e -> e.getName().equalsIgnoreCase(tableModel.getName()))) {
			throw new RuntimeException("Error: enum with name " + tableModel.getName() + " already exists!");
		}
		tables.add(tableModel);
		return tableModel;
	}

	public void addReverseReferenceField(TableModel tableA, String fieldA, TableModel tableB, String fieldB) {
		ReferenceFieldModel referenceFieldA = tableA.getReferenceField(fieldA);
		ReferenceFieldModel referenceFieldB = tableB.getReferenceField(fieldB);
		if (referenceFieldA == null || referenceFieldB == null) {
			throw new RuntimeException("Error missing reference fields for model:" + fieldA + ", " + fieldB);
		}
		referenceFieldA.setReverseReferenceField(referenceFieldB);
	}

	public String getName() {
		return name;
	}

	public String getTitle() {
		return title;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getModelClassName() {
		return modelClassName;
	}

	public String getFullNameSpace() {
		return getNamespace() + "." + getName().toLowerCase();
	}

	public List<TableModel> getTables() {
		return new ArrayList<>(tables);
	}

	public List<TableModel> getLocalTables() {
		return tables.stream().filter(t -> !t.isRemoteTable()).toList();
	}

	public List<TableModel> getRemoteTables() {
		return tables.stream().filter(TableModel::isRemoteTable).toList();
	}

	public TableModel getTable(String name) {
		return tables.stream()
				.filter(t -> t.getName().equals(name))
				.findAny()
				.orElse(null);
	}

	public FieldModel getField(String tableName, String fieldName) {
		TableModel table = getTable(tableName);
		return table != null ? table.getField(fieldName) : null;
	}

	public ReferenceFieldModel getReferenceField(String tableName, String fieldName) {
		TableModel table = getTable(tableName);
		return table != null ? table.getReferenceField(fieldName) : null;
	}

	public List<ViewModel> getViews() {
		return new ArrayList<>(views);
	}

	public List<TableModel> getImportedTables() {
		return tables.stream()
				.filter(TableModel::isRemoteTable)
				.toList();
	}

	public List<String> getImportedDatabases() {
		return getImportedTables().stream()
				.map(TableModel::getRemoteDatabase)
				.distinct()
				.toList();
	}

	public int getVersion() {
		return version;
	}

	private void setVersion(int version) {
		this.version = version;
	}

	public int getDateCreated() {
		return dateCreated;
	}

	private void setDateCreated(int dateCreated) {
		this.dateCreated = dateCreated;
	}

	public int getDateModified() {
		return dateModified;
	}

	private void setDateModified(int dateModified) {
		this.dateModified = dateModified;
	}

	public long getPojoBuildTime() {
		return pojoBuildTime;
	}

	public void setPojoBuildTime(long pojoBuildTime) {
		this.pojoBuildTime = pojoBuildTime;
	}

	public byte[] toBytes() throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		write(dos);
		dos.close();
		return bos.toByteArray();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Database model: ").append(name).append(", ").append(title).append(", ns:").append(namespace).append(", build:").append(pojoBuildTime).append(", version:").append(version).append("\n");
		sb.append("Tables: ").append(tables.size()).append("\n");
		for (TableModel table : tables) {
			sb.append("\t").append(table.getName()).append(", ").append(table.getTitle()).append(", (").append(table.getTableId()).append(")").append("\n");
			for (FieldModel field : table.getFields()) {
				sb.append("\t").append("\t").append(field.getName()).append(", ").append(field.getTitle()).append(", ").append(field.getFieldType()).append(", (").append(field.getFieldId()).append(")").append("\n");
			}
		}
		return sb.toString();
	}
}
