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
package org.teamapps.universaldb.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.teamapps.commons.util.collections.ByKeyComparisonResult;
import org.teamapps.commons.util.collections.CollectionUtil;
import org.teamapps.universaldb.UniversalDB;
import org.teamapps.universaldb.context.UserContext;
import org.teamapps.universaldb.index.binary.BinaryIndex;
import org.teamapps.universaldb.index.bool.BooleanIndex;
import org.teamapps.universaldb.index.buffer.index.RecordIndex;
import org.teamapps.universaldb.index.file.FileIndex;
import org.teamapps.universaldb.index.numeric.*;
import org.teamapps.universaldb.index.reference.CyclicReferenceUpdate;
import org.teamapps.universaldb.index.reference.ReferenceIndex;
import org.teamapps.universaldb.index.reference.multi.MultiReferenceIndex;
import org.teamapps.universaldb.index.reference.single.SingleReferenceIndex;
import org.teamapps.universaldb.index.text.CollectionTextSearchIndex;
import org.teamapps.universaldb.index.text.FullTextIndexValue;
import org.teamapps.universaldb.index.text.TextFilter;
import org.teamapps.universaldb.index.text.TextIndex;
import org.teamapps.universaldb.index.translation.TranslatableText;
import org.teamapps.universaldb.index.translation.TranslatableTextIndex;
import org.teamapps.universaldb.index.versioning.RecordVersioningIndex;
import org.teamapps.universaldb.model.FieldModel;
import org.teamapps.universaldb.model.FieldType;
import org.teamapps.universaldb.model.FileFieldModel;
import org.teamapps.universaldb.model.TableModel;
import org.teamapps.universaldb.query.AndFilter;
import org.teamapps.universaldb.query.Filter;
import org.teamapps.universaldb.query.IndexFilter;
import org.teamapps.universaldb.query.OrFilter;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;

public class TableIndex implements MappedObject {
	private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final DatabaseIndex databaseIndex;
	private final TableModel tableModel;
	private final String name;
	private final File dataPath;
	private final File fullTextIndexPath;
	private final RecordIndex records;
	private final List<FieldIndex> fieldIndices;
	private final Map<String, FieldIndex<?, ?>> fieldIndexByName;
	private boolean keepDeletedRecords;
	private RecordIndex deletedRecords;
	private CollectionTextSearchIndex collectionTextSearchIndex;
	private List<String> fileFieldNames;
	private List<TextIndex> textFields;
	private List<TranslatableTextIndex> translatedTextFields;
	private RecordVersioningIndex recordVersioningIndex;
	private long lastFullTextIndexCheck;


	public TableIndex(DatabaseIndex databaseIndex, TableModel tableModel) {
		this.databaseIndex = databaseIndex;
		this.tableModel = tableModel;
		this.name = tableModel.getName();
		this.dataPath = new File(databaseIndex.getDataPath(), name);
		this.fullTextIndexPath = new File(databaseIndex.getFullTextIndexPath(), name);
		dataPath.mkdir();
		fullTextIndexPath.mkdir();
		records = new RecordIndex(dataPath, "coll-recs");

		fieldIndices = new ArrayList<>();
		fieldIndexByName = new HashMap<>();

		if (tableModel.isRecoverableRecords()) {
			keepDeletedRecords = true;
			deletedRecords = new RecordIndex(dataPath, "coll-del-recs");
		}
		new IndexMetaData(dataPath, name, getFQN(), 0, tableModel.getTableId());
		if (tableModel.isVersioning()) {
			this.recordVersioningIndex = new RecordVersioningIndex(this);
		}
		Runtime.getRuntime().addShutdownHook(new Thread(this::close));
	}

	public void installOrMerge(TableModel tableModel) {
		ByKeyComparisonResult<FieldIndex, FieldModel, String> compareResult = CollectionUtil.compareByKey(fieldIndices, tableModel.getFields(), FieldIndex::getName, FieldModel::getName, true);
		//unknown fields for this model
		if (!compareResult.getAEntriesNotInB().isEmpty()) {
			throw new RuntimeException("Unknown fields that are not within the model:" + compareResult.getAEntriesNotInB().stream().map(FieldIndex::getName).collect(Collectors.joining(", ")));
		}

		//existing fields
		for (FieldIndex<?, ?> fieldIndex : compareResult.getAEntriesInB()) {
			//todo: which fields must be updated? enums, ...
		}

		//new fields
		for (FieldModel fieldModel : compareResult.getBEntriesNotInA()) {
			FieldIndex<?, ?> fieldIndex = createField(this, fieldModel);
			addIndex(fieldIndex);
		}
	}

	private FieldIndex createField(TableIndex tableIndex, FieldModel fieldModel) {
		FieldIndex<?, ?> column = null;

		switch (fieldModel.getFieldType().getIndexType()) {
			case BOOLEAN:
				column = new BooleanIndex(fieldModel, tableIndex);
				break;
			case SHORT:
				column = new ShortIndex(fieldModel, tableIndex);
				break;
			case INT:
				column = new IntegerIndex(fieldModel, tableIndex);
				break;
			case LONG:
				column = new LongIndex(fieldModel, tableIndex);
				break;
			case FLOAT:
				column = new FloatIndex(fieldModel, tableIndex);
				break;
			case DOUBLE:
				column = new DoubleIndex(fieldModel, tableIndex);
				break;
			case TEXT:
				column = new TextIndex(fieldModel, tableIndex, tableIndex.getCollectionTextSearchIndex());
				break;
			case TRANSLATABLE_TEXT:
				column = new TranslatableTextIndex(fieldModel, tableIndex, tableIndex.getCollectionTextSearchIndex());
				break;
			case REFERENCE:
				column = new SingleReferenceIndex(fieldModel, tableIndex);
				break;
			case MULTI_REFERENCE:
				column = new MultiReferenceIndex(fieldModel, tableIndex);
				break;
			case FILE:
				column = new FileIndex((FileFieldModel) fieldModel, tableIndex);
				break;
			case BINARY:
				column = new BinaryIndex(fieldModel, tableIndex);
				break;
		}
		return column;
	}



	public void addIndex(FieldIndex index) {
		fieldIndices.add(index);
		fieldIndexByName.put(index.getName(), index);
		fileFieldNames = null;
		textFields = null;
	}

	public CollectionTextSearchIndex getCollectionTextSearchIndex() {
		if (collectionTextSearchIndex == null) {
			collectionTextSearchIndex = new CollectionTextSearchIndex(fullTextIndexPath, "coll-text");
		}
		return collectionTextSearchIndex;
	}

	public void checkFullTextIndex() {
		if (collectionTextSearchIndex == null) {
			return;
		}
		if (
				(!records.getBoolean(0) && getCount() > 0 && (System.currentTimeMillis() - lastFullTextIndexCheck > 300_000)) ||
				getCount() > 0 && collectionTextSearchIndex.getMaxDoc() == 0
		) {
			long time = System.currentTimeMillis();
			logger.warn("RECREATING FULL TEXT INDEX FOR: " + getName() + " (RECORDS:" + getCount() + ", MAX-DOC:" + collectionTextSearchIndex.getMaxDoc() + ")");
			recreateFullTextIndex();
			lastFullTextIndexCheck = System.currentTimeMillis();
			logger.warn("RECREATING FINISHED FOR: " + getName() + " (TIME:" + (System.currentTimeMillis() - time) + ")");
		}
		records.setBoolean(0, false);
	}

	public void forceFullTextIndexRecreation() {
		logger.warn("FORCED RECREATING FULL TEXT INDEX FOR: " + getName() + " (RECORDS:" + getCount() + ", MAX-DOC:" + collectionTextSearchIndex.getMaxDoc() + ")");
		recreateFullTextIndex();

	}

	private void recreateFullTextIndex() {
		try {
			collectionTextSearchIndex.deleteAllDocuments();
			BitSet bitSet = records.getBitSet();
			for (int id = bitSet.nextSetBit(0); id >= 0; id = bitSet.nextSetBit(id + 1)) {
				List<FullTextIndexValue> values = new ArrayList<>();
				for (TextIndex textField : getTextFields()) {
					String value = textField.getValue(id);
					if (value != null) {
						values.add(new FullTextIndexValue(textField.getName(), value));
					}
				}
				for (TranslatableTextIndex translatableTextIndex : getTranslatedTextFields()) {
					TranslatableText value = translatableTextIndex.getValue(id);
					if (value != null) {
						values.add(new FullTextIndexValue(translatableTextIndex.getName(), value));
					}
				}
				if (!values.isEmpty()) {
					collectionTextSearchIndex.setRecordValues(id, values, false);
				}
			}
			collectionTextSearchIndex.commit(false);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public TableModel getTableModel() {
		return tableModel;
	}

	public RecordVersioningIndex getRecordVersioningIndex() {
		return recordVersioningIndex;
	}

	public File getDataPath() {
		return dataPath;
	}

	public File getFullTextIndexPath() {
		return fullTextIndexPath;
	}

	public BitSet getRecords() {
		return records.getBitSet();
	}

	public boolean isStored(int id) {
		return records.getBoolean(id);
	}

	public int getCount() {
		return records.getCount();
	}

	public BitSet getDeletedRecords() {
		if (!keepDeletedRecords) {
			return null;
		}
		return deletedRecords.getBitSet();
	}

	public int getDeletedRecordsCount() {
		return keepDeletedRecords ? deletedRecords.getCount() : 0;
	}

	public boolean isDeleted(int id) {
		if (deletedRecords != null) {
			return deletedRecords.getBoolean(id);
		} else {
			return !records.getBoolean(id);
		}
	}

	public Filter createFullTextFilter(String query, String... fieldNames) {
		return createFullTextFilter(query, null, fieldNames);
	}

	public Filter createFullTextFilter(String query, UserContext userContext, String... fieldNames) {
		AndFilter andFilter = new AndFilter();
		if (query == null || query.isBlank()) {
			return andFilter;
		}
		String[] terms = query.split(" ");
		for (String term : terms) {
			if (!term.isBlank()) {
				boolean isNegation = term.startsWith("!");
				TextFilter textFilter = parseTextFilter(term);
				if (userContext != null) {
					textFilter.setUserContext(userContext);
				}
				Filter fullTextFilter = createFullTextFilter(textFilter, !isNegation, fieldNames);
				andFilter.and(fullTextFilter);
			}
		}
		return andFilter;
	}

	private TextFilter parseTextFilter(String term) {
		boolean negation = false;
		boolean similar = false;
		boolean startsWith = false;
		boolean equals = false;
		if (term.startsWith("!")) {
			negation = true;
			term = term.substring(1);
		}
		if (term.endsWith("+")) {
			similar = true;
			term = term.substring(0, term.length() - 1);
		}
		if (term.endsWith("*")) {
			startsWith = true;
			term = term.substring(0, term.length() - 1);
		}
		if (term.contains("\"")) {
			equals = true;
			term = term.replace("\"", "");
		}
		if (equals) {
			return negation ? TextFilter.termEqualsFilter(term) : TextFilter.termNotEqualsFilter(term);
		} else if (similar) {
			return negation ? TextFilter.termNotSimilarFilter(term) : TextFilter.termSimilarFilter(term);
		} else if (startsWith) {
			return negation ? TextFilter.termStartsNotWithFilter(term) : TextFilter.termStartsWithFilter(term);
		} else {
			return negation ? TextFilter.termContainsNotFilter(term) : TextFilter.termContainsFilter(term);
		}
	}

	public Filter createFullTextFilter(TextFilter textFilter, String... fieldNames) {
		return createFullTextFilter(textFilter, true, fieldNames);
	}

	public Filter createFullTextFilter(TextFilter textFilter, boolean orQuery, String... fieldNames) {
		Filter filter = orQuery ? new OrFilter() : new AndFilter();
		if (fieldNames == null || fieldNames.length == 0) {
			fieldIndices.stream().filter(columnIndex -> columnIndex.getType() == IndexType.TEXT || columnIndex.getType() == IndexType.TRANSLATABLE_TEXT).forEach(columnIndex -> {
				IndexFilter<TextIndex, TextFilter> indexFilter = new IndexFilter<TextIndex, TextFilter>(columnIndex, textFilter);
				if (orQuery) {
					filter.or(indexFilter);
				} else {
					filter.and(indexFilter);
				}
			});
		} else {
			for (String fieldName : fieldNames) {
				FieldIndex fieldIndex = fieldIndexByName.get(fieldName);
				if (fieldIndex != null && fieldIndex.getType() == IndexType.TEXT || fieldIndex.getType() == IndexType.TRANSLATABLE_TEXT) {
					IndexFilter<TextIndex, TextFilter> indexFilter = new IndexFilter<TextIndex, TextFilter>(fieldIndex, textFilter);
					if (orQuery) {
						filter.or(indexFilter);
					} else {
						filter.and(indexFilter);
					}
				}
			}
		}
		return filter;
	}

	public List<SortEntry> sortRecords(String columnName, BitSet records, boolean ascending, UserContext userContext, SingleReferenceIndex... path) {
		FieldIndex index = null;
		if (path != null && path.length > 0) {
			index = path[path.length - 1].getReferencedTable().getFieldIndex(columnName);
		} else {
			index = getFieldIndex(columnName);
		}
		if (index == null) {
			return null;
		}
		List<SortEntry> sortEntries = SortEntry.createSortEntries(records, path);

		return index.sortRecords(sortEntries, ascending, userContext);
	}

	public int createRecord(int recordId) {
		int id = 0;
		if (recordId == 0) {
			if (keepDeletedRecords) {
				id = Math.max(records.getNextAvailableId(), deletedRecords.getNextAvailableId());
			} else {
				id = records.getNextAvailableId();
			}
		} else {
			id = recordId;
			if (keepDeletedRecords && deletedRecords.getBoolean(recordId)) {
				deletedRecords.setBoolean(recordId, false);
			}
		}
		records.setBoolean(id, true);
		return id;
	}

	public void updateFullTextIndex(int id, List<FullTextIndexValue> values, boolean update) {
		if (update) {
			Set<String> textFieldNames = values.stream().map(FullTextIndexValue::getFieldName).collect(Collectors.toSet());
			List<FullTextIndexValue> recordFullTextIndexValues = new ArrayList<>(values);
			for (TextIndex textField : getTextFields()) {
				if (!textFieldNames.contains(textField.getName())) {
					recordFullTextIndexValues.add(new FullTextIndexValue(textField.getName(), textField.getValue(id)));
				}
			}
			for (TranslatableTextIndex translatableTextIndex : getTranslatedTextFields()) {
				if (!textFieldNames.contains(translatableTextIndex.getName())) {
					TranslatableText translatableTextValue = translatableTextIndex.getValue(id);
					if (translatableTextValue != null) {
						recordFullTextIndexValues.add(new FullTextIndexValue(translatableTextIndex.getName(), translatableTextValue));
					}
				}
			}
			collectionTextSearchIndex.setRecordValues(id, recordFullTextIndexValues, true);
		} else {
			collectionTextSearchIndex.setRecordValues(id, values, false);
		}
	}

	private List<Integer> getReferencedRecords(int id, FieldIndex<?, ?> referenceColumn) {
		if (referenceColumn.getFieldType() == FieldType.MULTI_REFERENCE) {
			MultiReferenceIndex multiReferenceIndex = (MultiReferenceIndex) referenceColumn;
			return multiReferenceIndex.getReferencesAsList(id);
		} else {
			SingleReferenceIndex singleReferenceIndex = (SingleReferenceIndex) referenceColumn;
			int reference = singleReferenceIndex.getValue(id);
			return reference > 0 ? Collections.singletonList(reference) : Collections.emptyList();
		}
	}

	public List<CyclicReferenceUpdate> deleteRecord(int id) {
		return deleteRecord(id, null);
	}

	private List<CyclicReferenceUpdate> deleteRecord(int id, FieldIndex<?, ?> cascadeOriginIndex) {
		records.setBoolean(id, false);
		if (keepDeletedRecords) {
			if (deletedRecords.getBoolean(id)) {
				return Collections.emptyList();
			}
			deletedRecords.setBoolean(id, true);
		}
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();
		for (FieldIndex<?, ?> referenceColumn : getReferenceFields()) {
			ReferenceIndex referenceIndex = (ReferenceIndex) referenceColumn;
			if (referenceColumn == cascadeOriginIndex || referenceIndex.getReferenceFieldModel().getReferencedTable().isRemoteTable()) {
				continue;
			}
			boolean isCascadeDelete = referenceIndex.isCascadeDeleteReferences();
			TableIndex referencedTable = referenceIndex.getReferencedTable();
			boolean isReferenceKeepDeletedRecords = referencedTable.isKeepDeletedRecords();
			boolean isMultiReference = referenceIndex.isMultiReference();
			FieldIndex<?, ?> backReferenceColumn = referenceColumn.getReferencedColumn();
			boolean isWithBackReferenceColumn = backReferenceColumn != null;
			boolean isMultiBackReference = backReferenceColumn != null && backReferenceColumn.getFieldType() == FieldType.MULTI_REFERENCE;

			List<Integer> referencedRecords = getReferencedRecords(id, referenceColumn);
			if (referencedRecords.isEmpty()) {
				continue;
			}

			if (keepDeletedRecords) {
				if (isReferenceKeepDeletedRecords) {
					if (isCascadeDelete) {
						//remove no reference!
						//cascade delete
						referencedRecords.forEach(refId -> referencedTable.deleteRecord(refId, backReferenceColumn));
					} else {
						//remove back references
						if (isWithBackReferenceColumn) {
							List<CyclicReferenceUpdate> referenceUpdates = removeBackReferences(id, backReferenceColumn, isMultiBackReference, referencedRecords);
							cyclicReferenceUpdates.addAll(referenceUpdates);
						}
					}
				} else {
					if (isCascadeDelete) {
						//remove references
						if (isMultiReference) {
							MultiReferenceIndex multiReferenceColumn = (MultiReferenceIndex) referenceColumn;
							multiReferenceColumn.removeAllReferences(id, false);
						} else {
							SingleReferenceIndex singleReferenceColumn = (SingleReferenceIndex) referenceColumn;
							singleReferenceColumn.setValue(id, 0, false);
						}
						//cascade delete
						referencedRecords.forEach(referencedTable::deleteRecord);
					} else {
						//remove back references
						if (isWithBackReferenceColumn) {
							removeBackReferences(id, backReferenceColumn, isMultiBackReference, referencedRecords);
						}
					}
				}
			} else {
				if (isCascadeDelete) {
					referencedRecords.forEach(referencedTable::deleteRecord);
				}
			}
		}

		if (!keepDeletedRecords) {
			for (FieldIndex<?, ?> fieldIndex : fieldIndices) {
				fieldIndex.removeValue(id);
			}
			if (collectionTextSearchIndex != null) {
				collectionTextSearchIndex.delete(id, getFileFieldNames());
			}
		}
		return cyclicReferenceUpdates;
	}

	private List<CyclicReferenceUpdate> removeBackReferences(int id, FieldIndex<?, ?> backReferenceColumn, boolean isMultiBackReference, List<Integer> referencedRecords) {
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();
		if (isMultiBackReference) {
			MultiReferenceIndex multiBackReferenceColumn = (MultiReferenceIndex) backReferenceColumn;
			referencedRecords.forEach(refId -> {
				multiBackReferenceColumn.removeReferences(refId, Collections.singletonList(id), true);
				cyclicReferenceUpdates.add(new CyclicReferenceUpdate(multiBackReferenceColumn, true, refId, id));
			});
		} else {
			SingleReferenceIndex singleBackReferenceColumn = (SingleReferenceIndex) backReferenceColumn;
			referencedRecords.forEach(refId -> {
				int value = singleBackReferenceColumn.getValue(refId);
				if (id == value) {
					singleBackReferenceColumn.setValue(refId, 0, true);
					cyclicReferenceUpdates.add(new CyclicReferenceUpdate(singleBackReferenceColumn, true, refId, id));
				}
			});
		}
		return cyclicReferenceUpdates;
	}

	public List<CyclicReferenceUpdate> restoreRecord(int id) {
		return restoreRecord(id, null);
	}

	public List<CyclicReferenceUpdate> restoreRecord(int id, FieldIndex<?, ?> cascadeOriginIndex) {
		if (!keepDeletedRecords || !deletedRecords.getBoolean(id)) {
			return Collections.emptyList();
		}
		deletedRecords.setBoolean(id, false);
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();

		for (FieldIndex<?, ?> referenceColumn : getReferenceFields()) {
			ReferenceIndex referenceIndex = (ReferenceIndex) referenceColumn;
			if (referenceColumn == cascadeOriginIndex  || referenceIndex.getReferenceFieldModel().getReferencedTable().isRemoteTable()) {
				continue;
			}
			boolean isCascadeDelete = referenceIndex.isCascadeDeleteReferences();
			TableIndex referencedTable = referenceIndex.getReferencedTable();
			boolean isReferenceKeepDeletedRecords = referencedTable.isKeepDeletedRecords();
			boolean isMultiReference = referenceIndex.isMultiReference();
			FieldIndex<?, ?> backReferenceColumn = referenceColumn.getReferencedColumn();
			boolean isWithBackReferenceColumn = backReferenceColumn != null;
			boolean isMultiBackReference = backReferenceColumn != null && backReferenceColumn.getFieldType().isMultiReference();

			List<Integer> referencedRecords = getReferencedRecords(id, referenceColumn);
			if (referencedRecords.isEmpty()) {
				continue;
			}

			if (isReferenceKeepDeletedRecords) {
				if (isCascadeDelete) {
					//references are in place
					//cascade restore
					referencedRecords.forEach(refId -> referencedTable.restoreRecord(refId, backReferenceColumn));
				} else {
					//restore back references
					if (isWithBackReferenceColumn) {
						List<CyclicReferenceUpdate> referenceUpdates = restoreBackReferences(id, referenceColumn, referencedTable, isMultiReference, backReferenceColumn, isMultiBackReference, referencedRecords);
						cyclicReferenceUpdates.addAll(referenceUpdates);
					}
				}
			} else {
				if (isCascadeDelete) {
					//cascade deleted records are gone - nothing to do
				} else {
					if (isWithBackReferenceColumn) {
						//restore back references
						restoreBackReferences(id, referenceColumn, referencedTable, isMultiReference, backReferenceColumn, isMultiBackReference, referencedRecords);
					}
				}
			}
		}

		records.setBoolean(id, true);
		return cyclicReferenceUpdates;
	}

	private List<CyclicReferenceUpdate> restoreBackReferences(int id, FieldIndex<?, ?> referenceColumn, TableIndex referencedTable, boolean isMultiReference, FieldIndex<?, ?> backReferenceColumn, boolean isMultiBackReference, List<Integer> referencedRecords) {
		List<CyclicReferenceUpdate> cyclicReferenceUpdates = new ArrayList<>();
		if (isMultiBackReference) {
			MultiReferenceIndex multiBackReferenceColumn = (MultiReferenceIndex) backReferenceColumn;
			referencedRecords.forEach(refId -> {
				if (referencedTable.isStored(refId)) {
					//recreate back references
					multiBackReferenceColumn.addReferences(refId, Collections.singletonList(id), true);
					cyclicReferenceUpdates.add(new CyclicReferenceUpdate(multiBackReferenceColumn, false, refId, id));
				} else {
					//referenced entity was deleted so remove local refs
					if (isMultiReference) {
						MultiReferenceIndex multiReferenceColumn = (MultiReferenceIndex) referenceColumn;
						multiReferenceColumn.removeAllReferences(id, true);
					} else {
						SingleReferenceIndex singleReferenceColumn = (SingleReferenceIndex) referenceColumn;
						singleReferenceColumn.setValue(id, 0, true);
					}
				}
			});
		} else {
			SingleReferenceIndex singleBackReferenceColumn = (SingleReferenceIndex) backReferenceColumn;
			referencedRecords.forEach(refId -> {
				if (referencedTable.isStored(refId)) {
					int value = singleBackReferenceColumn.getValue(refId);
					if (value == 0) {
						//recreate back references
						singleBackReferenceColumn.setValue(refId, id, true);
						cyclicReferenceUpdates.add(new CyclicReferenceUpdate(singleBackReferenceColumn, false, refId, id));
					} else {
						//referenced entity was deleted so remove local refs
						if (isMultiReference) {
							MultiReferenceIndex multiReferenceColumn = (MultiReferenceIndex) referenceColumn;
							multiReferenceColumn.removeAllReferences(id, true);
						} else {
							SingleReferenceIndex singleReferenceColumn = (SingleReferenceIndex) referenceColumn;
							singleReferenceColumn.setValue(id, 0, true);
						}
					}
				}
			});
		}
		return cyclicReferenceUpdates;
	}


	private List<String> getFileFieldNames() {
		if (fileFieldNames == null) {
			fileFieldNames = fieldIndices.stream()
					.filter(index -> index.getType() == IndexType.FILE)
					.map(FieldIndex::getName)
					.collect(Collectors.toList());
		}
		return fileFieldNames;
	}

	private List<TextIndex> getTextFields() {
		if (textFields == null) {
			textFields = fieldIndices.stream()
					.filter(index -> index.getType() == IndexType.TEXT)
					.map(index -> (TextIndex) index)
					.collect(Collectors.toList());
		}
		return textFields;
	}

	private List<TranslatableTextIndex> getTranslatedTextFields() {
		if (translatedTextFields == null) {
			translatedTextFields = fieldIndices.stream()
					.filter(index -> index.getType() == IndexType.TRANSLATABLE_TEXT)
					.map(index -> (TranslatableTextIndex) index)
					.collect(Collectors.toList());
		}
		return translatedTextFields;
	}

	public BitSet getRecordBitSet() {
		return records.getBitSet();
	}

	public BitSet getDeletedRecordsBitSet() {
		if (!keepDeletedRecords) {
			return null;
		}
		return deletedRecords.getBitSet();
	}

	public List<FieldIndex> getFieldIndices() {
		return fieldIndices;
	}

	public List<ReferenceIndex<?, ?>> getReferenceFields() {
		return fieldIndices.stream()
				.filter(fieldIndex -> fieldIndex.getFieldType().isReference())
				.map(f -> (ReferenceIndex<?, ?>) f)
				.collect(Collectors.toList());
	}

	public FieldIndex getFieldIndex(String name) {
		return fieldIndexByName.get(name);
	}

	public boolean isKeepDeletedRecords() {
		return keepDeletedRecords;
	}

	public int getMappingId() {
		return tableModel.getTableId();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("collection: ").append(name).append(", id:").append(getMappingId()).append("\n");
		for (FieldIndex<?, ?> column : fieldIndices) {
			sb.append("\t").append(column.toString()).append("\n");
		}
		return sb.toString();
	}

	public void close() {
		try {
			logger.info(UniversalDB.SKIP_DB_LOGGING, "Shutdown on collection:" + name);
			if (collectionTextSearchIndex != null) {
				collectionTextSearchIndex.commit(true);
			}
			records.setBoolean(0, true);
			records.close();
			for (FieldIndex<?, ?> column : fieldIndices) {
				column.close();
			}
			if (recordVersioningIndex != null) {
				recordVersioningIndex.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void drop() {
		collectionTextSearchIndex.drop();
		for (FieldIndex column : fieldIndices) {
			column.drop();
		}
	}

	public String getFQN() {
		return databaseIndex.getName() + "." + name;
	}

	public String getName() {
		return name;
	}

	public DatabaseIndex getDatabaseIndex() {
		return databaseIndex;
	}
}
